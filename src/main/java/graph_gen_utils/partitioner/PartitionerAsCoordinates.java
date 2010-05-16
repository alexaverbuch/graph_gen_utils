package graph_gen_utils.partitioner;

import graph_gen_utils.general.Consts;
import graph_gen_utils.general.NodeData;

import java.util.ArrayList;

public class PartitionerAsCoordinates implements Partitioner {
  
  private ArrayList<PartitionZone> zones = new ArrayList<PartitionZone>();
  
  private double northWesternLon = -180;
  private double northWesternLat = 90;
  private double southEasternLon = 180;
  private double southEasternLat = -90;
  private byte ptns = 1;
  
  public enum BorderType {
    NORTH_SOUTH_BORDERS, EAST_WEST_BORDERS
  }
  
  public PartitionerAsCoordinates(double northWesternLon,
    double northWesternLat, double southEasternLon, double southEasternLat,
    BorderType borderType, byte ptns) {
    
    super();
    
    this.northWesternLon = northWesternLon;
    this.northWesternLat = northWesternLat;
    this.southEasternLon = southEasternLon;
    this.southEasternLat = southEasternLat;
    this.ptns = ptns;
    
    switch (borderType) {
      case NORTH_SOUTH_BORDERS:
        createNorthSouthBorderZones();
        break;
      
      case EAST_WEST_BORDERS:
        createEastWestBorderZones();
        break;
    }
    
  }
  
  private void createNorthSouthBorderZones() {
    
    double lonRange = (southEasternLon + 180) - (northWesternLon + 180);
    double zoneLonRange = lonRange / ptns;
    
    for (byte ptn = 0; ptn < ptns; ptn++) {
      double zoneMinLon = northWesternLon + (zoneLonRange * ptn);
      double zoneMaxLon = zoneMinLon + zoneLonRange;
      double zoneMinLat = southEasternLat;
      double zoneMaxLat = northWesternLat;
      
      zones.add(new PartitionZone(zoneMinLat, zoneMaxLat, zoneMinLon,
        zoneMaxLon, ptn));
    }
    
  }
  
  private void createEastWestBorderZones() {
    
    double latRange = (northWesternLat + 90) - (southEasternLat + 90);
    double zoneLatRange = latRange / ptns;
    
    for (byte ptn = 0; ptn < ptns; ptn++) {
      double zoneMinLon = northWesternLon;
      double zoneMaxLon = southEasternLon;
      double zoneMinLat = southEasternLat + (zoneLatRange * ptn);
      double zoneMaxLat = zoneMinLat + zoneLatRange;
      
      zones.add(new PartitionZone(zoneMinLat, zoneMaxLat, zoneMinLon,
        zoneMaxLon, ptn));
    }
    
  }
  
  @Override
  public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {
    
    for (NodeData tempNode : nodes) {
      
      double lat = (Double) tempNode.getProperties().get(Consts.LATITUDE);
      double lon = (Double) tempNode.getProperties().get(Consts.LONGITUDE);
      
      byte color = -1;
      for (PartitionZone zone : zones) {
        if (zone.responsibleFor(lat, lon) == false)
          continue;
        
        color = zone.getPartition();
        break;
      }
      
      if (color == -1) {
        System.err.printf("\nNode not coloured: GID[%d] lon[%f] lat[%f]",
          tempNode.getProperties().get(Consts.NODE_GID), lon, lat);
      }
      
      tempNode.getProperties().put(Consts.COLOR, color);
    }
    
    return nodes;
    
  }
  
  private class PartitionZone {
    
    double minLat = 0d;
    double maxLat = 0d;
    double minLon = 0d;
    double maxLon = 0d;
    byte partition = 0;
    
    public PartitionZone(double minLat, double maxLat, double minLon,
      double maxLon, byte partition) {
      this.minLat = minLat;
      this.maxLat = maxLat;
      this.minLon = minLon;
      this.maxLon = maxLon;
      this.partition = partition;
    }
    
    public boolean responsibleFor(double lat, double lon) {
      return ((minLat < lat) && (lat <= maxLat) && (minLon < lon) && (lon <= maxLon));
    }
    
    public byte getPartition() {
      return partition;
    }
    
  }
  
}
