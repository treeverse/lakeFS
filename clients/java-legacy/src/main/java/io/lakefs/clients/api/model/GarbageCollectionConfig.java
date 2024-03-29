/*
 * lakeFS API
 * lakeFS HTTP API
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.lakefs.clients.api.model;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * GarbageCollectionConfig
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class GarbageCollectionConfig {
  public static final String SERIALIZED_NAME_GRACE_PERIOD = "grace_period";
  @SerializedName(SERIALIZED_NAME_GRACE_PERIOD)
  private Integer gracePeriod;


  public GarbageCollectionConfig gracePeriod(Integer gracePeriod) {
    
    this.gracePeriod = gracePeriod;
    return this;
  }

   /**
   * Duration in seconds. Objects created in the recent grace_period will not be collected.
   * @return gracePeriod
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "Duration in seconds. Objects created in the recent grace_period will not be collected.")

  public Integer getGracePeriod() {
    return gracePeriod;
  }


  public void setGracePeriod(Integer gracePeriod) {
    this.gracePeriod = gracePeriod;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GarbageCollectionConfig garbageCollectionConfig = (GarbageCollectionConfig) o;
    return Objects.equals(this.gracePeriod, garbageCollectionConfig.gracePeriod);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gracePeriod);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GarbageCollectionConfig {\n");
    sb.append("    gracePeriod: ").append(toIndentedString(gracePeriod)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

