# GCP-logs-Export-Google-sheets

1- Clone this project on Your local machine.

2- Create a virtual environment using python -m venv <env-name>.
  
3- Activate the Environment.
  
4- Install All requiremnet mentioned in the requirements.txt using pip install -r requirements.txt.
  
5- Authorize service account And google SDK on your local machine "See google Docs for this purpose".
  
6- Change the filter according to Your requirement e.g "which kind of logs you want to filter and which service you are going to get log".
  
7- After run main.py 
  
8- ------------- End ----------
  
 >>>> - In this file I am filtering google cloud SQL and Compute Instance Logs and comparing these logs on pandas data frame.
  
 >>>> - You can choose any service you wanted to get log and then change that in filter "See google log language docs for this purpose".

  
>>>> - Get logs from google cloud different services such as cloud SQL or Compute Instance or any other service using google client library and then convert to pandas data frame and after will be exported to Google Sheets
