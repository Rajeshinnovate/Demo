using System;

using xAWS;
using static xAWS.cAWS;
using xAWS.xAPI.RequestLoggingApi;

using xCore;
using static xCore.modLogger;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.APIGatewayEvents;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using System.Net;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace RequestLogging
{
    public class RequestLogging
    {
        private readonly string MyS3Bucket;

        public RequestLogging()
        {
            try
            {
                Logger.Log("Enter");
                ymwCore.DataEnvironment = ymwCore.IsLambdaInstance == false ? ymwCore.EnumDataEnvironment.TST : GetDataEnvironment().ToUpper().ToEnumItem<ymwCore.EnumDataEnvironment>();
                string MyAccountNumber = TryGetEnvironmentVariable("AwsAccountNumber");
                MyS3Bucket = $"{GetDataEnvironment()}-rl-{InvokedFromRegion()}-{MyAccountNumber}-6ea16bf8f4614f78876526412".ToLower();
                LogFunctionInfo();
            }
            catch (Exception ex)
            {
                throw Logger.LogException_LogOnly(ex);
            }
            finally
            {
                Logger.Log("Exit");
            }
        }

        private string GetObjectExtension(LogRequest.DataType DataType)
        {
            switch (DataType)
            {
                case LogRequest.DataType.Unknown: return "";
                case LogRequest.DataType.JSON: return ".json";
                case LogRequest.DataType.Text: return ".txt";
                case LogRequest.DataType.UrlEncoded: return ".txt";
                case LogRequest.DataType.KVP: return ".txt";
                default: throw new Exception("Invalid: DataType");
            }
        }

        public async Task RequestLogging_Handler(SQSEvent Event)
        {
            try
            {
                Logger.Log("Enter");
                foreach (var MyMessage in Event.Records)
                {
                    LogRequest MyLoggingRequest = MyMessage.Body.DeserializeJSON<LogRequest>();
                    if (MyLoggingRequest.xTimestamp.IsNullOrWhiteSpace())
                        MyLoggingRequest.xTimestamp = DateTime.UtcNow.ToISO8601(MillisecondPrecision: 6);

                    string MyPath = $"{MyLoggingRequest.xService}/{MyLoggingRequest.xRefNum}/{MyLoggingRequest.xTimestamp}_{MyLoggingRequest.xDataLabel}{GetObjectExtension(MyLoggingRequest.xDataType)}";
                    await modIS3.S3_Upload_Async(modAWS.AWS_USWest2_DevCard(), MyS3Bucket, MyPath, MyLoggingRequest.xData.GetBytes());
                }
            }
            catch (Exception Ex)
            {
                Logger.LogException_LogOnly(Ex);
            }
            finally
            {
                Logger.Log("Exit");
            }
        }

        public APIGatewayProxyResponse RequestLoggingReader_Handler( APIGatewayProxyRequest request, ILambdaContext lambdaContext)
        {
            try
            {
                Logger.Log("Enter");
                string[] specificValidIps = new string[]
                {
                    "213.147.103.82" //Mono Development
                };
                cAWS.VerifyIP(request.RequestContext.Identity.SourceIp, true, false, specificValidIps );

                string refNum = request.PathParameters["xRefNum"];
                if (string.IsNullOrEmpty(refNum))
                    throw new ArgumentException("Missing xRefNum");
                Logger.Log($"xRefNum={refNum}");

                var resultList = new List<object>();
                modAWS.AWSClient awsClient = modAWS.AWS_USWest2_DevCard();

                // First get list of all services - first part of the prefix
                var bucketFolders = modIS3.S3_ListSubfolders(awsClient, MyS3Bucket, "");
                if (bucketFolders.Count == 0)
                {
                    Logger.Log("Root S3_ListSubfolders returned no results");
                }
                foreach (string folder in bucketFolders) {

                    var folderResult = awsClient.S3_ListObjects(MyS3Bucket, $"{folder}/{refNum}/");
                    foreach (string key in folderResult)
                    {
                        string[] keyParts = key.Split("/_".ToCharArray());

                        string logData = awsClient.S3_Download(MyS3Bucket, key);
                        dynamic logObj = (dynamic)logData;
                        // if the data is json, prevent double json formatting by deserializing into anonymous jobject
                        if (key.EndsWith(".json", StringComparison.CurrentCultureIgnoreCase))
                        {
                            logObj = logData.DeserializeJSON<dynamic>();
                        }
                        resultList.Add(
                                new
                                {
                                    Key = key,
                                    Service = keyParts[0],
                                    Timestamp = keyParts.Length > 1 ? keyParts[2] : "",
                                    DataLabel = keyParts.Length > 2 ? keyParts[3].Substring(0, keyParts[3].IndexOf('.')) : "",
                                    Data = logObj
                                });
                    }
                }

                return new APIGatewayProxyResponse
                {
                    StatusCode = (int)HttpStatusCode.OK,
                    Body = resultList.ToJSON(),
                    Headers = new Dictionary<string, string> {
                        { "Content-Type", "application/json" },
                        { "Access-Control-Allow-Origin", "*" },
                        { "Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token" },
                        { "Access-Control-Allow-Methods", "*" }
                    }
                };
            }
            catch (Exception ex)
            {
                Logger.LogException_LogOnly(ex);
                return new APIGatewayProxyResponse
                {
                    StatusCode = (int)HttpStatusCode.InternalServerError,
                    Body = $@"{{""Error"": ""{ex.Message}""}}",
                    Headers = new Dictionary<string, string> {
                        { "Content-Type", "application/json" },
                        { "Access-Control-Allow-Origin", "*" },
                        { "Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token" },
                        { "Access-Control-Allow-Methods", "*" }
                    }
                };
            }
            finally
            {
                Logger.Log("Exit");
            }
        }
    }
}
