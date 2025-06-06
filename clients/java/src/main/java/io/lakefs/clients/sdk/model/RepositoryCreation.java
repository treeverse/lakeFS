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


package io.lakefs.clients.sdk.model;

import java.util.Objects;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.lakefs.clients.sdk.JSON;

/**
 * RepositoryCreation
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class RepositoryCreation {
  public static final String SERIALIZED_NAME_NAME = "name";
  @SerializedName(SERIALIZED_NAME_NAME)
  private String name;

  public static final String SERIALIZED_NAME_STORAGE_ID = "storage_id";
  @SerializedName(SERIALIZED_NAME_STORAGE_ID)
  private String storageId;

  public static final String SERIALIZED_NAME_STORAGE_NAMESPACE = "storage_namespace";
  @SerializedName(SERIALIZED_NAME_STORAGE_NAMESPACE)
  private String storageNamespace;

  public static final String SERIALIZED_NAME_DEFAULT_BRANCH = "default_branch";
  @SerializedName(SERIALIZED_NAME_DEFAULT_BRANCH)
  private String defaultBranch;

  public static final String SERIALIZED_NAME_SAMPLE_DATA = "sample_data";
  @SerializedName(SERIALIZED_NAME_SAMPLE_DATA)
  private Boolean sampleData = false;

  public static final String SERIALIZED_NAME_READ_ONLY = "read_only";
  @SerializedName(SERIALIZED_NAME_READ_ONLY)
  private Boolean readOnly = false;

  public RepositoryCreation() {
  }

  public RepositoryCreation name(String name) {
    
    this.name = name;
    return this;
  }

   /**
   * Get name
   * @return name
  **/
  @javax.annotation.Nonnull
  public String getName() {
    return name;
  }


  public void setName(String name) {
    this.name = name;
  }


  public RepositoryCreation storageId(String storageId) {
    
    this.storageId = storageId;
    return this;
  }

   /**
   * Unique identifier of the underlying data store. *EXPERIMENTAL*
   * @return storageId
  **/
  @javax.annotation.Nullable
  public String getStorageId() {
    return storageId;
  }


  public void setStorageId(String storageId) {
    this.storageId = storageId;
  }


  public RepositoryCreation storageNamespace(String storageNamespace) {
    
    this.storageNamespace = storageNamespace;
    return this;
  }

   /**
   * Filesystem URI to store the underlying data in (e.g. \&quot;s3://my-bucket/some/path/\&quot;)
   * @return storageNamespace
  **/
  @javax.annotation.Nonnull
  public String getStorageNamespace() {
    return storageNamespace;
  }


  public void setStorageNamespace(String storageNamespace) {
    this.storageNamespace = storageNamespace;
  }


  public RepositoryCreation defaultBranch(String defaultBranch) {
    
    this.defaultBranch = defaultBranch;
    return this;
  }

   /**
   * Get defaultBranch
   * @return defaultBranch
  **/
  @javax.annotation.Nullable
  public String getDefaultBranch() {
    return defaultBranch;
  }


  public void setDefaultBranch(String defaultBranch) {
    this.defaultBranch = defaultBranch;
  }


  public RepositoryCreation sampleData(Boolean sampleData) {
    
    this.sampleData = sampleData;
    return this;
  }

   /**
   * Get sampleData
   * @return sampleData
  **/
  @javax.annotation.Nullable
  public Boolean getSampleData() {
    return sampleData;
  }


  public void setSampleData(Boolean sampleData) {
    this.sampleData = sampleData;
  }


  public RepositoryCreation readOnly(Boolean readOnly) {
    
    this.readOnly = readOnly;
    return this;
  }

   /**
   * Get readOnly
   * @return readOnly
  **/
  @javax.annotation.Nullable
  public Boolean getReadOnly() {
    return readOnly;
  }


  public void setReadOnly(Boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * A container for additional, undeclared properties.
   * This is a holder for any undeclared properties as specified with
   * the 'additionalProperties' keyword in the OAS document.
   */
  private Map<String, Object> additionalProperties;

  /**
   * Set the additional (undeclared) property with the specified name and value.
   * If the property does not already exist, create it otherwise replace it.
   *
   * @param key name of the property
   * @param value value of the property
   * @return the RepositoryCreation instance itself
   */
  public RepositoryCreation putAdditionalProperty(String key, Object value) {
    if (this.additionalProperties == null) {
        this.additionalProperties = new HashMap<String, Object>();
    }
    this.additionalProperties.put(key, value);
    return this;
  }

  /**
   * Return the additional (undeclared) property.
   *
   * @return a map of objects
   */
  public Map<String, Object> getAdditionalProperties() {
    return additionalProperties;
  }

  /**
   * Return the additional (undeclared) property with the specified name.
   *
   * @param key name of the property
   * @return an object
   */
  public Object getAdditionalProperty(String key) {
    if (this.additionalProperties == null) {
        return null;
    }
    return this.additionalProperties.get(key);
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RepositoryCreation repositoryCreation = (RepositoryCreation) o;
    return Objects.equals(this.name, repositoryCreation.name) &&
        Objects.equals(this.storageId, repositoryCreation.storageId) &&
        Objects.equals(this.storageNamespace, repositoryCreation.storageNamespace) &&
        Objects.equals(this.defaultBranch, repositoryCreation.defaultBranch) &&
        Objects.equals(this.sampleData, repositoryCreation.sampleData) &&
        Objects.equals(this.readOnly, repositoryCreation.readOnly)&&
        Objects.equals(this.additionalProperties, repositoryCreation.additionalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, storageId, storageNamespace, defaultBranch, sampleData, readOnly, additionalProperties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RepositoryCreation {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    storageId: ").append(toIndentedString(storageId)).append("\n");
    sb.append("    storageNamespace: ").append(toIndentedString(storageNamespace)).append("\n");
    sb.append("    defaultBranch: ").append(toIndentedString(defaultBranch)).append("\n");
    sb.append("    sampleData: ").append(toIndentedString(sampleData)).append("\n");
    sb.append("    readOnly: ").append(toIndentedString(readOnly)).append("\n");
    sb.append("    additionalProperties: ").append(toIndentedString(additionalProperties)).append("\n");
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


  public static HashSet<String> openapiFields;
  public static HashSet<String> openapiRequiredFields;

  static {
    // a set of all properties/fields (JSON key names)
    openapiFields = new HashSet<String>();
    openapiFields.add("name");
    openapiFields.add("storage_id");
    openapiFields.add("storage_namespace");
    openapiFields.add("default_branch");
    openapiFields.add("sample_data");
    openapiFields.add("read_only");

    // a set of required properties/fields (JSON key names)
    openapiRequiredFields = new HashSet<String>();
    openapiRequiredFields.add("name");
    openapiRequiredFields.add("storage_namespace");
  }

 /**
  * Validates the JSON Element and throws an exception if issues found
  *
  * @param jsonElement JSON Element
  * @throws IOException if the JSON Element is invalid with respect to RepositoryCreation
  */
  public static void validateJsonElement(JsonElement jsonElement) throws IOException {
      if (jsonElement == null) {
        if (!RepositoryCreation.openapiRequiredFields.isEmpty()) { // has required fields but JSON element is null
          throw new IllegalArgumentException(String.format("The required field(s) %s in RepositoryCreation is not found in the empty JSON string", RepositoryCreation.openapiRequiredFields.toString()));
        }
      }

      // check to make sure all required properties/fields are present in the JSON string
      for (String requiredField : RepositoryCreation.openapiRequiredFields) {
        if (jsonElement.getAsJsonObject().get(requiredField) == null) {
          throw new IllegalArgumentException(String.format("The required field `%s` is not found in the JSON string: %s", requiredField, jsonElement.toString()));
        }
      }
        JsonObject jsonObj = jsonElement.getAsJsonObject();
      if (!jsonObj.get("name").isJsonPrimitive()) {
        throw new IllegalArgumentException(String.format("Expected the field `name` to be a primitive type in the JSON string but got `%s`", jsonObj.get("name").toString()));
      }
      if ((jsonObj.get("storage_id") != null && !jsonObj.get("storage_id").isJsonNull()) && !jsonObj.get("storage_id").isJsonPrimitive()) {
        throw new IllegalArgumentException(String.format("Expected the field `storage_id` to be a primitive type in the JSON string but got `%s`", jsonObj.get("storage_id").toString()));
      }
      if (!jsonObj.get("storage_namespace").isJsonPrimitive()) {
        throw new IllegalArgumentException(String.format("Expected the field `storage_namespace` to be a primitive type in the JSON string but got `%s`", jsonObj.get("storage_namespace").toString()));
      }
      if ((jsonObj.get("default_branch") != null && !jsonObj.get("default_branch").isJsonNull()) && !jsonObj.get("default_branch").isJsonPrimitive()) {
        throw new IllegalArgumentException(String.format("Expected the field `default_branch` to be a primitive type in the JSON string but got `%s`", jsonObj.get("default_branch").toString()));
      }
  }

  public static class CustomTypeAdapterFactory implements TypeAdapterFactory {
    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
       if (!RepositoryCreation.class.isAssignableFrom(type.getRawType())) {
         return null; // this class only serializes 'RepositoryCreation' and its subtypes
       }
       final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
       final TypeAdapter<RepositoryCreation> thisAdapter
                        = gson.getDelegateAdapter(this, TypeToken.get(RepositoryCreation.class));

       return (TypeAdapter<T>) new TypeAdapter<RepositoryCreation>() {
           @Override
           public void write(JsonWriter out, RepositoryCreation value) throws IOException {
             JsonObject obj = thisAdapter.toJsonTree(value).getAsJsonObject();
             obj.remove("additionalProperties");
             // serialize additional properties
             if (value.getAdditionalProperties() != null) {
               for (Map.Entry<String, Object> entry : value.getAdditionalProperties().entrySet()) {
                 if (entry.getValue() instanceof String)
                   obj.addProperty(entry.getKey(), (String) entry.getValue());
                 else if (entry.getValue() instanceof Number)
                   obj.addProperty(entry.getKey(), (Number) entry.getValue());
                 else if (entry.getValue() instanceof Boolean)
                   obj.addProperty(entry.getKey(), (Boolean) entry.getValue());
                 else if (entry.getValue() instanceof Character)
                   obj.addProperty(entry.getKey(), (Character) entry.getValue());
                 else {
                   obj.add(entry.getKey(), gson.toJsonTree(entry.getValue()).getAsJsonObject());
                 }
               }
             }
             elementAdapter.write(out, obj);
           }

           @Override
           public RepositoryCreation read(JsonReader in) throws IOException {
             JsonElement jsonElement = elementAdapter.read(in);
             validateJsonElement(jsonElement);
             JsonObject jsonObj = jsonElement.getAsJsonObject();
             // store additional fields in the deserialized instance
             RepositoryCreation instance = thisAdapter.fromJsonTree(jsonObj);
             for (Map.Entry<String, JsonElement> entry : jsonObj.entrySet()) {
               if (!openapiFields.contains(entry.getKey())) {
                 if (entry.getValue().isJsonPrimitive()) { // primitive type
                   if (entry.getValue().getAsJsonPrimitive().isString())
                     instance.putAdditionalProperty(entry.getKey(), entry.getValue().getAsString());
                   else if (entry.getValue().getAsJsonPrimitive().isNumber())
                     instance.putAdditionalProperty(entry.getKey(), entry.getValue().getAsNumber());
                   else if (entry.getValue().getAsJsonPrimitive().isBoolean())
                     instance.putAdditionalProperty(entry.getKey(), entry.getValue().getAsBoolean());
                   else
                     throw new IllegalArgumentException(String.format("The field `%s` has unknown primitive type. Value: %s", entry.getKey(), entry.getValue().toString()));
                 } else if (entry.getValue().isJsonArray()) {
                     instance.putAdditionalProperty(entry.getKey(), gson.fromJson(entry.getValue(), List.class));
                 } else { // JSON object
                     instance.putAdditionalProperty(entry.getKey(), gson.fromJson(entry.getValue(), HashMap.class));
                 }
               }
             }
             return instance;
           }

       }.nullSafe();
    }
  }

 /**
  * Create an instance of RepositoryCreation given an JSON string
  *
  * @param jsonString JSON string
  * @return An instance of RepositoryCreation
  * @throws IOException if the JSON string is invalid with respect to RepositoryCreation
  */
  public static RepositoryCreation fromJson(String jsonString) throws IOException {
    return JSON.getGson().fromJson(jsonString, RepositoryCreation.class);
  }

 /**
  * Convert an instance of RepositoryCreation to an JSON string
  *
  * @return JSON string
  */
  public String toJson() {
    return JSON.getGson().toJson(this);
  }
}

