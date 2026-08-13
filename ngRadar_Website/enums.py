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
    UI = 101, "User Interface"
    ETR = 102, "E-Transfer Progress Writer"


class Status(models.IntegerChoices):
    READY = 1, "Ready"
    QUEUED = 2, "Queued"                # Won't worry about this status for now, but it would be used if we had a queue of incoming e-transfers to process
    BLOCKED = 3, "Blocked"              # Won't worry about this status for now, I feel like it would be closely tied to the QUEUED status.
    TRANSFERRING = 4, "Transferring"    # Used when the e-transfer is actively in progress. 
    VERIFYING = 5, "Verifying"          # Will verify the number of bytes received at DSOC matches the expected number of bytes being sent from VLBA. 
    TRANSFERRED = 6, "Transferred"      # This status would be used when the e-transfer has completed from etc -> etd successfully, will be the status sent by kafka to DSOC to begin DSOC workflow.
    FAILED = 7, "Failed"        
    COMPLETED = 8, "Completed"          # This status would be used when the e-transfer has completed successfully and the data has been verified, processed, and stored appropriately.

