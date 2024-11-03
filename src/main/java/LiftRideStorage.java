import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import model.LiftRide;

@Slf4j
public class LiftRideStorage {
  private final ConcurrentHashMap<Integer, List<LiftRide>> skierLiftRides = new ConcurrentHashMap<>();
  public void addLiftRide(int skierId, LiftRide liftRide) {
    skierLiftRides.computeIfAbsent(skierId, k -> new CopyOnWriteArrayList<>()).add(liftRide);
    //log.info("skierLiftRides map: {}", skierLiftRides);
  }

  public List<LiftRide> getLiftRidesForSkier(int skierId) {
    return skierLiftRides.getOrDefault(skierId, List.of());
  }


}
