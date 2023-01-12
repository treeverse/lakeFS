package azure

//import (
//	"os"
//	"time"
//
//	"github.com/Azure/azure-storage-blob-go/azblob"
//
//	autorestAzure "github.com/Azure/go-autorest/autorest/azure"
//	"github.com/Azure/go-autorest/autorest/azure/auth"
//	"github.com/treeverse/lakefs/pkg/logging"
//)
//
//func getTokenRefresher() (azblob.TokenRefresher, error) {
//	// storage account and key not set, try and get credentials with azure authentication
//	msiConf := auth.NewMSIConfig()
//	msiConf.Resource = autorestAzure.PublicCloud.ResourceIdentifiers.Storage
//	azureServicePrincipalToken, err := msiConf.ServicePrincipalToken()
//	if err != nil {
//		return nil, err
//	}
//	return func(credential azblob.TokenCredential) time.Duration {
//		if err := azureServicePrincipalToken.Refresh(); err != nil {
//			logging.Default().WithError(err).Error("failed on token refresh")
//		}
//		refreshedToken := azureServicePrincipalToken.Token()
//		credential.SetToken(refreshedToken.OAuthToken())
//		return time.Until(refreshedToken.Expires())
//	}, nil
//}
//
//func GetAccessKeyCredentials(accountName, accountKey string) (azblob.Credential, error) {
//	if len(accountName) == 0 && len(accountKey) == 0 {
//		// fallback to Azure environment variables
//		accountName, accountKey = os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
//	}
//	return azblob.NewSharedKeyCredential(accountName, accountKey)
//}
//
//func GetMSICredentials() (azblob.Credential, error) {
//	tokenRefresher, err := getTokenRefresher()
//	if err != nil {
//		return nil, err
//	}
//	return azblob.NewTokenCredential("", tokenRefresher), nil
//}
