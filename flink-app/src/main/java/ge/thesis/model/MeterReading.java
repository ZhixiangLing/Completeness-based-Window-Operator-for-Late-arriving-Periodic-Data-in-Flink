package ge.thesis.model;

public class MeterReading {
        public String seriesId;
        public String locationId; 
        public String meterId;
        public long eventT;
        public double value;
        public String valueStatus;
        public long insertT;
        public long extractT;

        public MeterReading() {
        }
                
        public MeterReading(String seriesId, String locationId, String meterId, long eventT, double value, String valueStatus, long insertT, long extractT) {
            this.seriesId = seriesId;
            this.locationId = locationId;
            this.meterId = meterId;
            this.eventT = eventT;
            this.value = value;
            this.valueStatus = valueStatus;
            this.insertT = insertT;
            this.extractT = extractT;
        }

        @Override
        public String toString() {
            return "MeterReading{" +
                "seriesId='" + seriesId + '\'' +
                ", locationId='" + locationId + '\'' +
                ", meterId='" + meterId + '\'' +
                ", eventTime=" + eventT +
                ", value=" + value +
                ", valueStatus='" + valueStatus + '\'' +
                '}';
        }
        
    }
