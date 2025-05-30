variable "extraction_lambda_eventbridge" {
 description = "Triggers lambda extraction every job every 5 minutes"
 default     = "extraction_lambda_eventbridge"
}


variable "extraction_lambda_eventbus" {
 description = "The name of the Event bus"
 default     = "extraction_lambda_eventbus"
}


variable "common_tags" {
 description = "Common tags to be applied to all resources"
 type        = map(string)
 default     = {
   "Owner"  = "Tester"
   "Source" = "Terraform",
   "Group"  = "Test"
 }
}


variable "connection_api_key" {
 description = "The API key for the connection"
 default     = "e56a43ba8233450a56e2ecfeb1ada"
}


variable "destination_url" {
 description = "The URL of the API destination"
 default     = "https://example.com/test/slack"
}