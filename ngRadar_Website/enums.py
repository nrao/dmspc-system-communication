from django.db import models

class Stations(models.TextChoices):
    GBT = "GBT", "Green Bank (100-m, GBT)"
    SC = "SC", "St. Croix (25-m, VLBA)"
    HN = "HN", "Hancock (25-m, VLBA)"
    NL = "NL", "North Liberty (25-m, VLBA)"
    FD = "FD", "Fort Davis (25-m, VLBA)"
    LA  = "LA", "Los Alamos (25-m, VLBA)"
    PT  = "PT", "Pie Town (25-m, VLBA)"
    KP  = "KP", "Kitt Peak (25-m, VLBA)"
    OV  = "OV", "Owens Valley (25-m, VLBA)"
    BR  = "BR", "Brewster (25-m, VLBA)"
    MK  = "MK", "Mauna Kea (25-m, VLBA)"