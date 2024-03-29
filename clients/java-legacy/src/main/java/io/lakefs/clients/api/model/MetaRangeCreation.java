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
import io.lakefs.clients.api.model.RangeMetadata;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MetaRangeCreation
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class MetaRangeCreation {
  public static final String SERIALIZED_NAME_RANGES = "ranges";
  @SerializedName(SERIALIZED_NAME_RANGES)
  private List<RangeMetadata> ranges = new ArrayList<RangeMetadata>();


  public MetaRangeCreation ranges(List<RangeMetadata> ranges) {
    
    this.ranges = ranges;
    return this;
  }

  public MetaRangeCreation addRangesItem(RangeMetadata rangesItem) {
    this.ranges.add(rangesItem);
    return this;
  }

   /**
   * Get ranges
   * @return ranges
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public List<RangeMetadata> getRanges() {
    return ranges;
  }


  public void setRanges(List<RangeMetadata> ranges) {
    this.ranges = ranges;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetaRangeCreation metaRangeCreation = (MetaRangeCreation) o;
    return Objects.equals(this.ranges, metaRangeCreation.ranges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ranges);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class MetaRangeCreation {\n");
    sb.append("    ranges: ").append(toIndentedString(ranges)).append("\n");
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

