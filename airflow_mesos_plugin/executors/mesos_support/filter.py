from mesos.interface import mesos_pb2


class AirflowMesosFilterMode(object):
    INCLUDE = 'include'
    EXCLUDE = 'exclude'


class AirflowMesosFilter(object):
    def check(self, offer):
        raise NotImplementedError


class AirflowMesosOfferFilter(AirflowMesosFilter):
    def __init__(self, filters):
        self.filters = filters

    def check(self, offer):
        for offer_filter in self.filters:
            if not offer_filter.check(offer):
                return False
        return True


class AirflowMesosResourceFilter(AirflowMesosFilter):
    def __init__(self, name, value, mode=AirflowMesosFilterMode.INCLUDE):
        self.name = name
        self.value = value
        self.mode = mode

    def check(self, offer):
        offer_resource = self.get_resource(offer)
        if not offer_resource:
            return False

        resource_type = offer_resource.type
        if resource_type == mesos_pb2.Value.SCALAR:
            return self.check_scalar(offer, offer_resource)
        elif resource_type == mesos_pb2.Value.RANGE:
            return self.check_range(offer, offer_resource)
        elif resource_type == mesos_pb2.Value.RANGES:
            return self.check_ranges(offer, offer_resource)
        elif resource_type == mesos_pb2.Value.SET:
            return self.check_set(offer, offer_resource)
        elif resource_type == mesos_pb2.Value.TEXT:
            return self.check_text(offer, offer_resource)
        else:
            raise Exception("Can't handle type %s" % resource_type)

    def get_resource(self, offer):
        found = [resource for resource in offer.resources
                 if resource.name == self.name]

        if len(found) > 1:
            # There appears to be an assumption that this will not happen
            e = ("Found more than one resource for name %s for offer %s"
                 % (self.name, offer))
            raise Exception(e)

        if len(found) < 1:
            return None

        return found[0]

    def check_scalar(self, offer, offer_resource):
        resource_value = offer_resource.scalar.value

        if self.mode == AirflowMesosFilterMode.INCLUDE:
            return self.value <= resource_value
        elif self.mode == AirflowMesosFilterMode.EXCLUDE:
            return self.value > resource_value
        else:
            raise Exception("Unknown mode %s" % self.mode)

    def check_range(self, offer, offer_resource):
        resource_range = offer_resource.range
        in_range = resource_range.begin <= self.value <= resource_range.end

        if self.mode == AirflowMesosFilterMode.INCLUDE:
            return in_range
        elif self.mode == AirflowMesosFilterMode.EXCLUDE:
            return not in_range
        else:
            raise Exception("Only 'include' and 'exclude' are supported.")

    def check_ranges(self, offer, offer_resource):
        resource_ranges = offer_resource.ranges

        in_range = False
        for resource_range in resource_ranges:
            if resource_range.begin <= self.value <= resource_range.end:
                in_range = True
                break

        if self.mode == AirflowMesosFilterMode.INCLUDE:
            return in_range
        elif self.mode == AirflowMesosFilterMode.EXCLUDE:
            return not in_range
        else:
            raise Exception("Only 'include' and 'exclude' are supported.")

    def check_set(self, offer, offer_resource):
        raise NotImplementedError

    def check_text(self, offer, offer_resource):
        raise NotImplementedError


class AirflowMesosHostnameFilter(AirflowMesosFilter):
    def __init__(self, hostnames, exclude=False):
        self.hostnames = hostnames
        self.exclude = exclude

    def check(self, offer):
        has_hostname = offer.hostname in self.hostnames or \
            '*' in self.hostnames
        if not self.exclude:
            return has_hostname
        else:
            return not has_hostname
