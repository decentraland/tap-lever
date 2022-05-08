from singer_sdk import typing as th

schema = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("opportunityId", th.StringType),
    th.Property("createdAt", th.IntegerType),
    th.Property("creator", th.StringType),
    th.Property("status", th.StringType),
    th.Property(
        "fields",
        th.ArrayType(
            th.ObjectType(
                th.Property("text", th.StringType),
                th.Property("identifier", th.StringType),
                th.Property("value", th.StringType),
            )
        ),
    ),
    th.Property(
        "signatures",
        th.ObjectType(
            th.Property("role", th.StringType),
            th.Property("name", th.StringType),
            th.Property("email", th.StringType),
            th.Property("firstOpenedAt", th.IntegerType),
            th.Property("lastOpenedAt", th.IntegerType),
            th.Property("signedAt", th.IntegerType),
            th.Property("signed", th.BooleanType),
            th.Property(
                "candidate",
                th.ObjectType(
                    th.Property("email", th.StringType),
                    th.Property("lastOpenedAt", th.IntegerType),
                    th.Property("role", th.StringType),
                    th.Property("signedAt", th.IntegerType),
                    th.Property("firstOpenedAt", th.IntegerType),
                    th.Property("signed", th.BooleanType),
                    th.Property("name", th.StringType),
                ),
            ),
        ),
    ),
    th.Property("approvedAt", th.IntegerType),
    th.Property("sentAt", th.IntegerType),
    th.Property("approved", th.BooleanType),
    th.Property("posting", th.StringType),
    th.Property(
        "sentDocument",
        th.ObjectType(
            th.Property("uploadedAt", th.IntegerType),
            th.Property("downloadUrl", th.StringType),
            th.Property("fileName", th.StringType),
        ),
    ),
    th.Property(
        "signedDocument",
        th.ObjectType(
            th.Property("uploadedAt", th.IntegerType),
            th.Property("downloadUrl", th.StringType),
            th.Property("fileName", th.StringType),
        ),
    ),
).to_dict()
