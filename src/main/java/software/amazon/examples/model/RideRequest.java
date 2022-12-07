package software.amazon.examples.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@NoArgsConstructor
@ToString
public class RideRequest {

    private Long tripId;

    private Long rideRequestId;
    private Long vehicleId;
    private Double expectedFare;

    @JsonCreator
    public RideRequest(
        @JsonProperty("tripId") Long tripId,
        @JsonProperty("rideRequestId") Long rideRequestId,
        @JsonProperty("vehicleId") Long vehicleId,
        @JsonProperty("expectedFare") Double expectedFare) {
        this.tripId = tripId;
        this.rideRequestId = rideRequestId;
        this.vehicleId = vehicleId;
        this.expectedFare = expectedFare;
    }
}
