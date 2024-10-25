package common;

import java.io.Serializable;

public class GpsPoint implements Serializable {

    /**
     * Longitude.
     * The longitude of the prime meridian is 0°, and the longitude of other locations on Earth is either eastward to 180° or westward to 180°.
     */
    double longitude;

    /**
     * Latitude. The equatorial latitude is 0, the North Pole is 90 degrees north, and the South Pole is 90 degrees south.
     * So latitude and longitude increase from lower left to upper right
     */
    double latitude;

    public GpsPoint(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
}
