import datetime

from piccolo.columns.column_types import (
    UUID,
    ForeignKey,
    Integer,
    Varchar,
    Text,
    Timestamp,
    Timestamptz,
    Array,
    Float,
    Boolean
)
from piccolo.table import Table

class ScrapeData(Table):
    id = UUID(primary_key=True)
    title = Text()
    jobLocation = Text()
    postedDate = Timestamptz()
    detailsPageUrl = Text()
    companyPageUrl = Text()
    salary = Text()
    companyName = Text()
    summary = Text()
    description = Text(default="Scraping...")
    skills = Text(default="Scraping...")
    jobId = Text(unique=True, required=True)
    modifiedDate = Timestamptz()
    created_at = Timestamp(auto_update=datetime.datetime.now)
    companyLogoUrl = Text()
    clientBrandId = Text()
    employmentType = Text()
    score = Float()
    easyApply = Boolean()
    employerType = Text()
    workFromHomeAvailability = Text()
    isRemote = Boolean()
    guid = Text()
