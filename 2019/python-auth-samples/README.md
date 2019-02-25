# Python Authorization Samples

These code samples demonstrate different ways to make authenticated requests
to Google APIs with Python 3 and the ways that authentication might go wrong.

## Setup

Set up a local Python development environment for developing applications
that integrate with Google Cloud Platform.

* Follow the instructions at https://cloud.google.com/python/setup.

Install the necessary packages:

```
pip install -r requirements.txt
```

## API Keys

API Keys can be used only to access publically-readable resources such as
data exposed via the Google Maps API.

```
python api_key.py YOUR-API-KEY
```

When successful, this outputs:

```
{
   "results" : [
      {
         "address_components" : [
            {
               "long_name" : "1600",
               "short_name" : "1600",
               "types" : [ "street_number" ]
            },
        ...
         ]
      }
    ...
   ],
   "status": "OK"
}
```

### What if the Geocoding API isn't enabled on the project?

```
python api_key.py YOUR-API-KEY
```

This command outputs something like:

```
{
   "error_message" : "This API project is not authorized to use this API.",
   "results" : [],
   "status" : "REQUEST_DENIED"
}
```

The solution is to enable the API on the project that the API key is
associated with.

## User-based authentication

The `scopes.py` sample demonstrates what happens when insufficient scopes are
requested when accessing an API via end-user credentials.

```
python scopes.py
```

This command outputs something like:

```
{
  "error": {
    "code": 403,
    "message": "Request had insufficient authentication scopes.",
    "status": "PERMISSION_DENIED"
  }
}
```

**Exercise:** How would you change the code sample so that the request to get
Google Sheets data succeeds?

## Identity and Access Management (IAM)

The `iam.py` sample demonstrates what happens when the user account
associated with the access token attached to a request does not have the
right role to access a requested resource.

This example

```
python iam.py
```

This command outputs something like:

```
{
 "error": {
  "code": 403,
  "message": "acct@my-project-id.iam.gserviceaccount.com does not have
  storage.objects.get access to bucket-you-cant-access/README.txt."
 }
}
```

**Exercise:**

0.  Create a service account with minimal permissions and use it to
    authenticate in this code sample.
1.  Create a private bucket with a data file as an object.
2.  Verify that the service account you created cannot read the object by
    running this code sample.
3.  Grant the service account object reader role on the bucket.
4.  Verify that the service account can read the object by running this code
    sample.
