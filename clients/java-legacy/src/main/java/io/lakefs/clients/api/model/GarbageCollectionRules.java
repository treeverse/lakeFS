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
import io.lakefs.clients.api.model.GarbageCollectionRule;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GarbageCollectionRules
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class GarbageCollectionRules {
  public static final String SERIALIZED_NAME_DEFAULT_RETENTION_DAYS = "default_retention_days";
  @SerializedName(SERIALIZED_NAME_DEFAULT_RETENTION_DAYS)
  private Integer defaultRetentionDays;

  public static final String SERIALIZED_NAME_BRANCHES = "branches";
  @SerializedName(SERIALIZED_NAME_BRANCHES)
  private List<GarbageCollectionRule> branches = new ArrayList<GarbageCollectionRule>();


  public GarbageCollectionRules defaultRetentionDays(Integer defaultRetentionDays) {
    
    this.defaultRetentionDays = defaultRetentionDays;
    return this;
  }

   /**
   * Get defaultRetentionDays
   * @return defaultRetentionDays
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public Integer getDefaultRetentionDays() {
    return defaultRetentionDays;
  }


  public void setDefaultRetentionDays(Integer defaultRetentionDays) {
    this.defaultRetentionDays = defaultRetentionDays;
  }


  public GarbageCollectionRules branches(List<GarbageCollectionRule> branches) {
    
    this.branches = branches;
    return this;
  }

  public GarbageCollectionRules addBranchesItem(GarbageCollectionRule branchesItem) {
    this.branches.add(branchesItem);
    return this;
  }

   /**
   * Get branches
   * @return branches
  **/
  @javax.annotation.Nonnull
  @ApiModelProperty(required = true, value = "")

  public List<GarbageCollectionRule> getBranches() {
    return branches;
  }


  public void setBranches(List<GarbageCollectionRule> branches) {
    this.branches = branches;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GarbageCollectionRules garbageCollectionRules = (GarbageCollectionRules) o;
    return Objects.equals(this.defaultRetentionDays, garbageCollectionRules.defaultRetentionDays) &&
        Objects.equals(this.branches, garbageCollectionRules.branches);
  }

  @Override
  public int hashCode() {
    return Objects.hash(defaultRetentionDays, branches);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GarbageCollectionRules {\n");
    sb.append("    defaultRetentionDays: ").append(toIndentedString(defaultRetentionDays)).append("\n");
    sb.append("    branches: ").append(toIndentedString(branches)).append("\n");
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

