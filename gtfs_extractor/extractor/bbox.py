class Bbox:
    min_lat = None
    max_lat = None
    min_lon = None
    max_lon = None

    def contains(self, lat: float, lon: float):
        return lat <= self.max_lat and lat >= self.min_lat and lon <= self.max_lon and lon >= self.min_lon

    @staticmethod
    def create_from_coordinates(min_lon, min_lat, max_lon, max_lat):
        bbox = Bbox()
        bbox.min_lat = min_lat
        bbox.max_lat = max_lat
        bbox.min_lon = min_lon
        bbox.max_lon = max_lon
        return bbox
