"""
Customizable serialization.
"""
from bson.objectid import ObjectId
from django.db import models
from django.db.models.query import QuerySet
from mongoengine import Document
from mongoengine.queryset import QuerySet as MongoQuerySet
from django.utils.encoding import smart_unicode, is_protected_type, smart_str

import inspect
import types
import logging
logger = logging.getLogger()

# We register serializer classes, so that we can refer to them by their
# class names, if there are cyclical serialization heirachys.
_serializers = {}


def _field_to_tuple(field):
    """
    Convert an item in the `fields` attribute into a 2-tuple.
    """
    if isinstance(field, (tuple, list)):
        return (field[0], field[1])
    return (field, None)


def _fields_to_list(fields):
    """
    Return a list of field tuples.
    """
    return [_field_to_tuple(field) for field in fields or ()]


class _SkipField(Exception):
    """
    Signals that a serialized field should be ignored.
    We use this mechanism as the default behavior for ensuring
    that we don't infinitely recurse when dealing with nested data.
    """
    pass


class _RegisterSerializer(type):
    """
    Metaclass to register serializers.
    """
    def __new__(cls, name, bases, attrs):
        # Build the class and register it.
        ret = super(_RegisterSerializer, cls).__new__(cls, name, bases, attrs)
        _serializers[name] = ret
        return ret


class Serializer(object):
    """
    Converts python objects into plain old native types suitable for
    serialization.  In particular it handles models and querysets.

    The output format is specified by setting a number of attributes
    on the class.

    You may also override any of the serialization methods, to provide
    for more flexible behavior.

    Valid output types include anything that may be directly rendered into
    json, xml etc...
    """
    __metaclass__ = _RegisterSerializer

    fields = ()
    """
    Specify the fields to be serialized on a model or dict.
    Overrides `include` and `exclude`.
    """

    include = ()
    """
    Fields to add to the default set to be serialized on a model/dict.
    """

    exclude = ()
    """
    Fields to remove from the default set to be serialized on a model/dict.
    """

    rename = {}
    """
    A dict of key->name to use for the field keys.
    """

    related_serializer = None
    """
    The default serializer class to use for any related models.
    """

    depth = None
    """
    The maximum depth to serialize to, or `None`.
    """

    def __init__(self, depth=None, stack=[], **kwargs):
        if depth is not None:
            self.depth = depth
        self.stack = stack

    def get_fields(self, obj):
        fields = self.fields

        # If `fields` is not set, we use the default fields and modify
        # them with `include` and `exclude`
        if not fields:
            default = self.get_default_fields(obj)
            include = self.include or ()
            exclude = self.exclude or ()
            fields = set(default + list(include)) - set(exclude)

        return fields

    def get_default_fields(self, obj):
        """
        Return the default list of field names/keys for a model instance/dict.
        These are used if `fields` is not given.
        """
        if isinstance(obj, models.Model):
            opts = obj._meta
            return [field.name for field in opts.fields + opts.many_to_many]
        else:
            return obj.keys()

    def get_related_serializer(self, info):
        # If an element in `fields` is a 2-tuple of (str, tuple)
        # then the second element of the tuple is the fields to
        # set on the related serializer
        if isinstance(info, (list, tuple)):
            class OnTheFlySerializer(self.__class__):
                fields = info
            return OnTheFlySerializer

        # If an element in `fields` is a 2-tuple of (str, Serializer)
        # then the second element of the tuple is the Serializer
        # class to use for that field.
        elif isinstance(info, type) and issubclass(info, Serializer):
            return info

        # If an element in `fields` is a 2-tuple of (str, str)
        # then the second element of the tuple is the name of the Serializer
        # class to use for that field.
        #
        # Black magic to deal with cyclical Serializer dependancies.
        # Similar to what Django does for cyclically related models.
        elif isinstance(info, str) and info in _serializers:
            return _serializers[info]

        # Otherwise use `related_serializer` or fall back to `Serializer`
        return getattr(self, 'related_serializer') or Serializer

    def serialize_key(self, key):
        """
        Keys serialize to their string value,
        unless they exist in the `rename` dict.
        """
        return self.rename.get(smart_str(key), smart_str(key))

    def serialize_val(self, key, obj, related_info):
        """
        Convert a model field or dict value into a serializable representation.
        """
        related_serializer = self.get_related_serializer(related_info)

        if self.depth is None:
            depth = None
        elif self.depth <= 0:
            return self.serialize_max_depth(obj)
        else:
            depth = self.depth - 1

        if any([obj is elem for elem in self.stack]):
            return self.serialize_recursion(obj)
        else:
            stack = self.stack[:]
            stack.append(obj)

        return related_serializer(depth=depth, stack=stack).serialize(obj)

    def serialize_max_depth(self, obj):
        """
        Determine how objects should be serialized once `depth` is exceeded.
        The default behavior is to ignore the field.
        """
        raise _SkipField

    def serialize_recursion(self, obj):
        """
        Determine how objects should be serialized if recursion occurs.
        The default behavior is to ignore the field.
        """
        raise _SkipField

    def serialize_model(self, instance):
        """
        Given a model instance or dict, serialize it to a dict..
        """
        data = {}

        fields = self.get_fields(instance)

        # serialize each required field
        for fname, related_info in _fields_to_list(fields):
            try:
                # we first check for a method 'fname' on self,
                # 'fname's signature must be 'def fname(self, instance)'
                meth = getattr(self, fname, None)
                if (inspect.ismethod(meth) and
                            len(inspect.getargspec(meth)[0]) == 2):
                    obj = meth(instance)
                elif hasattr(instance, '__contains__') and fname in instance:
                    # then check for a key 'fname' on the instance
                    obj = instance[fname]
                elif hasattr(instance, smart_str(fname)):
                    # finally check for an attribute 'fname' on the instance
                    obj = getattr(instance, fname)
                else:
                    continue

                key = self.serialize_key(fname)
                val = self.serialize_val(fname, obj, related_info)
                data[key] = val
            except _SkipField:
                pass

        return data

    def serialize_iter(self, obj):
        """
        Convert iterables into a serializable representation.
        """
        return [self.serialize(item) for item in obj]

    def serialize_func(self, obj):
        """
        Convert no-arg methods and functions into a serializable representation.
        """
        return self.serialize(obj())

    def serialize_manager(self, obj):
        """
        Convert a model manager into a serializable representation.
        """
        return self.serialize_iter(obj.all())

    def serialize_fallback(self, obj):
        """
        Convert any unhandled object into a serializable representation.
        """
        return smart_unicode(obj, strings_only=True)


    def mongoengine_doc_to_dict(self,me_doc, sub_documents=False, mapping = {}):
        import copy, datetime, time
        from mongoengine import Document, ObjectIdField

        def _convert_to_json(me_doc):
            struct = {}
            ignore = ['_id', 'password']
            for k in me_doc:
                if k in ignore and not k in mapping: continue
                try:
                    value = me_doc[k]
                except:
                    value = k # not an array or dict

                # Map keys
                if k in mapping:
                    k = mapping[k]

                if sub_documents and hasattr(value, "__class__") and issubclass(value.__class__, Document):
                    struct[k] = _convert_to_json(value)
                elif isinstance(value, datetime.datetime):
                    struct[k] = value.strftime('%Y-%m-%dT%H:%M:%S.%fZ') # MJA 27apr12 - I prefer ISO datetime format here.  int(time.mktime(value.timetuple()) + value.microsecond/1e6)
                elif isinstance(value, (unicode, str)):
                    struct[k] = value
                elif isinstance(value, ObjectId):
                    struct[k] = str(value)
                elif isinstance(value, (dict,  tuple, int, long, float)):
                    # other serializable type, e.g. int.  (list of serializable types are at http://docs.python.org/library/json.html#json.JSONEncoder)
                    try:
                        struct[k] = value
                    except:
                        pass # unknown error putting the value in a dict, just swallow it.

            return struct

        return _convert_to_json(me_doc)

    def serialize(self, obj):
        """
        Convert any object into a serializable representation.
        """

        mapping  = {}
        try:
            mapping = self.mapping
        except:
            pass

        if isinstance(obj, (dict, models.Model)):
            # Model instances & dictionaries
            return self.serialize_model(obj)
        elif isinstance(obj, (tuple, list, set, QuerySet, types.GeneratorType)):
            # basic iterables
            return self.serialize_iter(obj)
        elif isinstance(obj, (MongoQuerySet)):
            # MongoQuerySet. Acts much like a list, each element being a mongoengine.document.Document-derived object
            try:
                # iterate through the queryset, serializing one mongoengine.document.Document-derived object at a time.
                json_info=[]
                for array_element in obj:
                    json_info .append( self.mongoengine_doc_to_dict(array_element, sub_documents=True, mapping = mapping) ) # todo iterate

            except Exception as e:
                logger.exception("Exception serializing MongoQuerySet")

            return json_info # self.serialize_iter(obj)
        elif issubclass(type(obj),Document):
            # convert the mongoengine.document.Document-derived object
            # used for e.g. http://127.0.0.1:8000/api/7ako4p1/
            #todo - test this is ok.
            json_info = self.mongoengine_doc_to_dict(obj, mapping = mapping)
            return json_info
        elif isinstance(obj, models.Manager):
            # Manager objects
            return self.serialize_manager(obj)
        elif inspect.isfunction(obj) and not inspect.getargspec(obj)[0]:
            # function with no args
            return self.serialize_func(obj)
        elif inspect.ismethod(obj) and len(inspect.getargspec(obj)[0]) <= 1:
            # bound method
            return self.serialize_func(obj)

        # Protected types are passed through as is.
        # (i.e. Primitives like None, numbers, dates, and Decimals.)
        if is_protected_type(obj):
            return obj

        # All other values are converted to string.
        return self.serialize_fallback(obj)
