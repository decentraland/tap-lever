from singer_sdk import typing as th

schema = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("opportunityId", th.StringType),
    th.Property("createdAt", th.IntegerType),
    th.Property("file", th.ObjectType()),
    th.Property("parsedData", th.ObjectType()),
).to_dict()
