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
 * Diff
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class Diff {
  /**
   * Gets or Sets type
   */
  @JsonAdapter(TypeEnum.Adapter.class)
  public enum TypeEnum {
    ADDED("added"),
    
    REMOVED("removed"),
    
    CHANGED("changed"),
    
    CONFLICT("conflict"),
    
    PREFIX_CHANGED("prefix_changed");

    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static TypeEnum fromValue(String value) {
      for (TypeEnum b : TypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<TypeEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final TypeEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public TypeEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return TypeEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_TYPE = "type";
  @SerializedName(SERIALIZED_NAME_TYPE)
  private TypeEnum type;

  public static final String SERIALIZED_NAME_PATH = "path";
  @SerializedName(SERIALIZED_NAME_PATH)
  private String path;

  /**
   * Gets or Sets pathType
   */
  @JsonAdapter(PathTypeEnum.Adapter.class)
  public enum PathTypeEnum {
    COMMON_PREFIX("common_prefix"),
    
    OBJECT("object");

    private String value;

    PathTypeEnum(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static PathTypeEnum fromValue(String value) {
      for (PathTypeEnum b : PathTypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

    public static class Adapter extends TypeAdapter<PathTypeEnum> {
      @Override
      public void write(final JsonWriter jsonWriter, final PathTypeEnum enumeration) throws IOException {
        jsonWriter.value(enumeration.getValue());
      }

      @Override
      public PathTypeEnum read(final JsonReader jsonReader) throws IOException {
        String value =  jsonReader.nextString();
        return PathTypeEnum.fromValue(value);
      }
    }
  }

  public static final String SERIALIZED_NAME_PATH_TYPE = "path_type";
  @SerializedName(SERIALIZED_NAME_PATH_TYPE)
  private PathTypeEnum pathType;

  public static final String SERIALIZED_NAME_SIZE_BYTES = "size_bytes";
  @SerializedName(SERIALIZED_NAME_SIZE_BYTES)
  private Long sizeBytes;


  public Diff type(TypeEnum type) {
    
    this.type = type;
    return this;
  }

   /**
   * Get type
   * @return type
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public TypeEnum getType() {
    return type;
  }


  public void setType(TypeEnum type) {
    this.type = type;
  }


  public Diff path(String path) {
    
    this.path = path;
    return this;
  }

   /**
   * Get path
   * @return path
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public String getPath() {
    return path;
  }


  public void setPath(String path) {
    this.path = path;
  }


  public Diff pathType(PathTypeEnum pathType) {
    
    this.pathType = pathType;
    return this;
  }

   /**
   * Get pathType
   * @return pathType
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public PathTypeEnum getPathType() {
    return pathType;
  }


  public void setPathType(PathTypeEnum pathType) {
    this.pathType = pathType;
  }


  public Diff sizeBytes(Long sizeBytes) {
    
    this.sizeBytes = sizeBytes;
    return this;
  }

   /**
   * represents the size of the added/changed/deleted entry
   * @return sizeBytes
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "represents the size of the added/changed/deleted entry")

  public Long getSizeBytes() {
    return sizeBytes;
  }


  public void setSizeBytes(Long sizeBytes) {
    this.sizeBytes = sizeBytes;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Diff diff = (Diff) o;
    return Objects.equals(this.type, diff.type) &&
        Objects.equals(this.path, diff.path) &&
        Objects.equals(this.pathType, diff.pathType) &&
        Objects.equals(this.sizeBytes, diff.sizeBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, path, pathType, sizeBytes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Diff {\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    path: ").append(toIndentedString(path)).append("\n");
    sb.append("    pathType: ").append(toIndentedString(pathType)).append("\n");
    sb.append("    sizeBytes: ").append(toIndentedString(sizeBytes)).append("\n");
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

