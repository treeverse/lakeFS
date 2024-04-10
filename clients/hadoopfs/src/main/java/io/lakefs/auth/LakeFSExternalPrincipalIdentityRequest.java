package io.lakefs.auth;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class LakeFSExternalPrincipalIdentityRequest {
 private final String method;
 private final String host;
 private final String region;
 private final String action;
 private final String date;
 @SerializedName("expiration_duration")
 private final String expirationDuration;
 @SerializedName("access_key_id")
 private final String accessKeyId;
 private final String signature;
 @SerializedName("signed_headers")
 private final List<String> signedHeaders;
 private final String version;
 private final String algorithm;
 @SerializedName("security_token")
 private final String securityToken;

 public LakeFSExternalPrincipalIdentityRequest(String method, String host, String region, String action, String date, String expirationDuration, String accessKeyId, String signature, List<String> signedHeaders, String version, String algorithm, String securityToken) {
  this.method = method;
  this.host = host;
  this.region = region;
  this.action = action;
  this.date = date;
  this.expirationDuration = expirationDuration;
  this.accessKeyId = accessKeyId;
  this.signature = signature;
  this.signedHeaders = signedHeaders;
  this.version = version;
  this.algorithm = algorithm;
  this.securityToken = securityToken;
 }
 public static LakeFSExternalPrincipalIdentityRequest fromJSON(String jsonString){
  Gson gson = new Gson();
  LakeFSExternalPrincipalIdentityRequest request = gson.fromJson(jsonString, LakeFSExternalPrincipalIdentityRequest.class);
  return request;
 }
 public String toJSON(){
  Gson gson = new Gson();
  String jsonString = gson.toJson(this);
  return jsonString;
 }

 public String getMethod() {
  return this.method;
 }

 public String getHost() {
  return this.host;
 }

 public String getRegion() {
  return this.region;
 }

 public String getAction() {
  return this.action;
 }

 public String getDate() {
  return this.date;
 }

 public String getExpirationDuration() {
  return this.expirationDuration;
 }

 public String getAccessKeyId() {
  return this.accessKeyId;
 }

 public String getSignature() {
  return this.signature;
 }

 public List<String> getSignedHeaders() {
  return this.signedHeaders;
 }

 public String getVersion() {
  return this.version;
 }

 public String getAlgorithm() {
  return this.algorithm;
 }

 public String getSecurityToken() {
  return this.securityToken;
 }
}

