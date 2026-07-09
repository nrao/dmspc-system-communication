from django.db import models

class Stations(models.IntegerChoices):
    GBT = 9,  "Green Bank (100-m, GBT)"
    SC  = 90, "St. Croix (25-m, VLBA)"
    HN  = 91, "Hancock (25-m, VLBA)"
    NL  = 92, "North Liberty (25-m, VLBA)"
    FD  = 93, "Fort Davis (25-m, VLBA)"
    LA  = 94, "Los Alamos (25-m, VLBA)"
    PT  = 95, "Pie Town (25-m, VLBA)"
    KP  = 96, "Kitt Peak (25-m, VLBA)"
    OV  = 97, "Owens Valley (25-m, VLBA)"
    BR  = 98, "Brewster (25-m, VLBA)"
    MK  = 99, "Mauna Kea (25-m, VLBA)"
    DSOC = 100, "DSOC (Domenici Socorro Operations Center)"