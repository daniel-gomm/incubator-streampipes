package org.apache.streampipes.examples.config;

import org.apache.streampipes.config.SpConfig;
import org.apache.streampipes.container.model.PeConfig;

import static org.apache.streampipes.examples.config.ConfigKeys.*;

public enum Config implements PeConfig {

  INSTANCE;

  private SpConfig config;

  public final static String serverUrl;
  public final static String iconBaseUrl;

  private final static String SERVICE_ID= "pe/org.apache.streampipes.examples";

  Config() {
    config = SpConfig.getSpConfig("pe/org.apache.streampipes.examples");

    config.register(HOST, "stateful-examples", "Hostname for the pe sinks");
    config.register(PORT, 8090, "Port for the pe sinks");

    config.register(ICON_HOST, "backend", "Hostname for the icon host");
    config.register(ICON_PORT, 80, "Port for the icons in nginx");

    config.register(SERVICE_NAME, "example", "The name of the service");
  }

  static {
    serverUrl = Config.INSTANCE.getHost() + ":" + Config.INSTANCE.getPort();
    iconBaseUrl = "http://" + Config.INSTANCE.getIconHost() + ":" + Config.INSTANCE.getIconPort() + "/assets/img/pe_icons";
  }

  public static final String getIconUrl(String pictureName) {
    return iconBaseUrl + "/" + pictureName + ".png";
  }

  public String getHost() {
    return config.getString(HOST);
  }

  public int getPort() {
    return config.getInteger(PORT);
  }

  public String getIconHost() {
    return config.getString(ICON_HOST);
  }

  public int getIconPort() {
    return config.getInteger(ICON_PORT);
  }

  @Override
  public String getId() {
    return SERVICE_ID;
  }

  @Override
  public String getName() {
    return config.getString(SERVICE_NAME);
  }

}
