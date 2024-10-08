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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * UpdateObjectUserMetadata
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class UpdateObjectUserMetadata {
  public static final String SERIALIZED_NAME_SET = "set";
  @SerializedName(SERIALIZED_NAME_SET)
  private Map<String, String> set = new HashMap<String, String>();


  public UpdateObjectUserMetadata set(Map<String, String> set) {
    
    this.set = set;
    return this;
  }

  public UpdateObjectUserMetadata putSetItem(String key, String setItem) {
    this.set.put(key, setItem);
    return this;
  }

   /**
   * Get set
   * @return set
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public Map<String, String> getSet() {
    return set;
  }


  public void setSet(Map<String, String> set) {
    this.set = set;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateObjectUserMetadata updateObjectUserMetadata = (UpdateObjectUserMetadata) o;
    return Objects.equals(this.set, updateObjectUserMetadata.set);
  }

  @Override
  public int hashCode() {
    return Objects.hash(set);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateObjectUserMetadata {\n");
    sb.append("    set: ").append(toIndentedString(set)).append("\n");
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

