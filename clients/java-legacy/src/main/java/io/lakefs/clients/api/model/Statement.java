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
import java.util.ArrayList;
import java.util.List;

/**
 * Statement
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class Statement {
  /**
   * Gets or Sets effect
   */
  @JsonAdapter(EffectEnum.Adapter.class)
  public enum EffectEnum {
    ALLOW("allow"),
    
    DENY("deny");

    private String value;

    EffectEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static EffectEnum fromValue(String value) {
      for (EffectEnum b : EffectEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<EffectEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final EffectEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public EffectEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return EffectEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_EFFECT = "effect";
  @SerializedName(SERIALIZED_NAME_EFFECT)
  private EffectEnum effect;

  public static final String SERIALIZED_NAME_RESOURCE = "resource";
  @SerializedName(SERIALIZED_NAME_RESOURCE)
  private String resource;

  public static final String SERIALIZED_NAME_ACTION = "action";
  @SerializedName(SERIALIZED_NAME_ACTION)
  private List<String> action = new ArrayList<String>();


  public Statement effect(EffectEnum effect) {
    
    this.effect = effect;
    return this;
  }

   /**
   * Get effect
   * @return effect
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public EffectEnum getEffect() {
    return effect;
  }


  public void setEffect(EffectEnum effect) {
    this.effect = effect;
  }


  public Statement resource(String resource) {
    
    this.resource = resource;
    return this;
  }

   /**
   * Get resource
   * @return resource
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public String getResource() {
    return resource;
  }


  public void setResource(String resource) {
    this.resource = resource;
  }


  public Statement action(List<String> action) {
    
    this.action = action;
    return this;
  }

  public Statement addActionItem(String actionItem) {
    this.action.add(actionItem);
    return this;
  }

   /**
   * Get action
   * @return action
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public List<String> getAction() {
    return action;
  }


  public void setAction(List<String> action) {
    this.action = action;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Statement statement = (Statement) o;
    return Objects.equals(this.effect, statement.effect) &&
        Objects.equals(this.resource, statement.resource) &&
        Objects.equals(this.action, statement.action);
  }

  @Override
  public int hashCode() {
    return Objects.hash(effect, resource, action);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Statement {\n");
    sb.append("    effect: ").append(toIndentedString(effect)).append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    action: ").append(toIndentedString(action)).append("\n");
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

