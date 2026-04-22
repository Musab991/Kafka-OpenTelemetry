namespace AvlSensorProducer.Models
{
    public class AvlRecord
    {
        public string VehicleId { get; set; } = string.Empty;
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public double Speed { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }
}
