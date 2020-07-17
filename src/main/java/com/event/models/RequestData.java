package com.event.models;

public class RequestData {
    private String meta_clientType;
    private String applicationConsumerKey;
    private String applicationName;
    private String applicationId;
    private String applicationOwner;
    private String apiContext;
    private String apiName;
    private String apiVersion;
    private String apiResourcePath;
    private String apiResourceTemplate;
    private String apiMethod;
    private String apiCreator;
    private String apiCreatorTenantDomain;
    private String apiTier;
    private String apiHostname;
    private String username;
    private String userTenantDomain;
    private String userIp;
    private String userAgent;
    private long requestTimestamp;
    private boolean throttledOut;
    private long responseTime;
    private long serviceTime;
    private long backendTime;
    private boolean responseCacheHit;
    private long responseSize;
    private String protocol;
    private int responseCode;
    private String destination;
    private long securityLatency;
    private long throttlingLatency;
    private long requestMedLat;
    private long responseMedLat;
    private long backendLatency;
    private long otherLatency;
    private String gatewayType;
    private String label;
    private String properties;

    public RequestData(String meta_clientType,
                       String applicationConsumerKey,
                       String applicationName,
                       String applicationId,
                       String applicationOwner,
                       String apiContext,
                       String apiName,
                       String apiVersion,
                       String apiResourcePath,
                       String apiResourceTemplate,
                       String apiMethod,
                       String apiCreator,
                       String apiCreatorTenantDomain,
                       String apiTier,
                       String apiHostname,
                       String username,
                       String userTenantDomain,
                       String userIp,
                       String userAgent,
                       long requestTimestamp,
                       boolean throttledOut,
                       long responseTime,
                       long serviceTime,
                       long backendTime,
                       boolean responseCacheHit,
                       long responseSize,
                       String protocol,
                       int responseCode,
                       String destination,
                       long securityLatency,
                       long throttlingLatency,
                       long requestMedLat,
                       long responseMedLat,
                       long backendLatency,
                       long otherLatency,
                       String gatewayType,
                       String label,
                       String properties) {
        this.meta_clientType = meta_clientType;
        this.applicationConsumerKey = applicationConsumerKey;
        this.applicationName = applicationName;
        this.applicationId = applicationId;
        this.applicationOwner = applicationOwner;
        this.apiContext = apiContext;
        this.apiName = apiName;
        this.apiVersion = apiVersion;
        this.apiResourcePath = apiResourcePath;
        this.apiResourceTemplate = apiResourceTemplate;
        this.apiMethod = apiMethod;
        this.apiCreator = apiCreator;
        this.apiCreatorTenantDomain = apiCreatorTenantDomain;
        this.apiTier = apiTier;
        this.apiHostname = apiHostname;
        this.username = username;
        this.userTenantDomain = userTenantDomain;
        this.userIp = userIp;
        this.userAgent = userAgent;
        this.requestTimestamp = requestTimestamp;
        this.throttledOut = throttledOut;
        this.responseTime = responseTime;
        this.serviceTime = serviceTime;
        this.backendTime = backendTime;
        this.responseCacheHit = responseCacheHit;
        this.responseSize = responseSize;
        this.protocol = protocol;
        this.responseCode = responseCode;
        this.destination = destination;
        this.securityLatency = securityLatency;
        this.throttlingLatency = throttlingLatency;
        this.requestMedLat = requestMedLat;
        this.responseMedLat = responseMedLat;
        this.backendLatency = backendLatency;
        this.otherLatency = otherLatency;
        this.gatewayType = gatewayType;
        this.label = label;
        this.properties = properties;
    }

    public String getMeta_clientType() {
        return meta_clientType;
    }

    public void setMeta_clientType(String meta_clientType) {
        this.meta_clientType = meta_clientType;
    }

    public String getApplicationConsumerKey() {
        return applicationConsumerKey;
    }

    public void setApplicationConsumerKey(String applicationConsumerKey) {
        this.applicationConsumerKey = applicationConsumerKey;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationOwner() {
        return applicationOwner;
    }

    public void setApplicationOwner(String applicationOwner) {
        this.applicationOwner = applicationOwner;
    }

    public String getApiContext() {
        return apiContext;
    }

    public void setApiContext(String apiContext) {
        this.apiContext = apiContext;
    }

    public String getApiName() {
        return apiName;
    }

    public void setApiName(String apiName) {
        this.apiName = apiName;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public String getApiResourcePath() {
        return apiResourcePath;
    }

    public void setApiResourcePath(String apiResourcePath) {
        this.apiResourcePath = apiResourcePath;
    }

    public String getApiResourceTemplate() {
        return apiResourceTemplate;
    }

    public void setApiResourceTemplate(String apiResourceTemplate) {
        this.apiResourceTemplate = apiResourceTemplate;
    }

    public String getApiMethod() {
        return apiMethod;
    }

    public void setApiMethod(String apiMethod) {
        this.apiMethod = apiMethod;
    }

    public String getApiCreator() {
        return apiCreator;
    }

    public void setApiCreator(String apiCreator) {
        this.apiCreator = apiCreator;
    }

    public String getApiCreatorTenantDomain() {
        return apiCreatorTenantDomain;
    }

    public void setApiCreatorTenantDomain(String apiCreatorTenantDomain) {
        this.apiCreatorTenantDomain = apiCreatorTenantDomain;
    }

    public String getApiTier() {
        return apiTier;
    }

    public void setApiTier(String apiTier) {
        this.apiTier = apiTier;
    }

    public String getApiHostname() {
        return apiHostname;
    }

    public void setApiHostname(String apiHostname) {
        this.apiHostname = apiHostname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUserTenantDomain() {
        return userTenantDomain;
    }

    public void setUserTenantDomain(String userTenantDomain) {
        this.userTenantDomain = userTenantDomain;
    }

    public String getUserIp() {
        return userIp;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public long getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(long requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public boolean isThrottledOut() {
        return throttledOut;
    }

    public void setThrottledOut(boolean throttledOut) {
        this.throttledOut = throttledOut;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }

    public long getServiceTime() {
        return serviceTime;
    }

    public void setServiceTime(long serviceTime) {
        this.serviceTime = serviceTime;
    }

    public long getBackendTime() {
        return backendTime;
    }

    public void setBackendTime(long backendTime) {
        this.backendTime = backendTime;
    }

    public boolean isResponseCacheHit() {
        return responseCacheHit;
    }

    public void setResponseCacheHit(boolean responseCacheHit) {
        this.responseCacheHit = responseCacheHit;
    }

    public long getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(long responseSize) {
        this.responseSize = responseSize;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public long getSecurityLatency() {
        return securityLatency;
    }

    public void setSecurityLatency(long securityLatency) {
        this.securityLatency = securityLatency;
    }

    public long getThrottlingLatency() {
        return throttlingLatency;
    }

    public void setThrottlingLatency(long throttlingLatency) {
        this.throttlingLatency = throttlingLatency;
    }

    public long getRequestMedLat() {
        return requestMedLat;
    }

    public void setRequestMedLat(long requestMedLat) {
        this.requestMedLat = requestMedLat;
    }

    public long getResponseMedLat() {
        return responseMedLat;
    }

    public void setResponseMedLat(long responseMedLat) {
        this.responseMedLat = responseMedLat;
    }

    public long getBackendLatency() {
        return backendLatency;
    }

    public void setBackendLatency(long backendLatency) {
        this.backendLatency = backendLatency;
    }

    public long getOtherLatency() {
        return otherLatency;
    }

    public void setOtherLatency(long otherLatency) {
        this.otherLatency = otherLatency;
    }

    public String getGatewayType() {
        return gatewayType;
    }

    public void setGatewayType(String gatewayType) {
        this.gatewayType = gatewayType;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }
}
