from events.common.fields import Field, Group, event_name, event_sub_type, event_type

event_start_date = Field(label="Start Date", value="s_date")
event_end_date = Field(label="End Date", value="e_date")
event_countries = Field(label="Countries", value="countries")
event_city = Field(label="City", value="city")


hover_group = Group(
    label="EVENT FIELDS",
    value="event_fields",
    fields=[event_name, event_type, event_sub_type, event_start_date, event_end_date, event_countries, event_city],
)
