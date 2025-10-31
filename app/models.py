"""
Data models and constants
"""

from dataclasses import dataclass
from datetime import datetime

# Event flow and timing
EVENT_FLOW = ["EVDAV", "EVIMC", "EVGPD", "ENKDN"]

EVENT_GAPS_HOURS = {
    "EVDAV->EVIMC": (1.0, 24.0),
    "EVIMC->EVGPD": (4.0, 36.0),
    "EVGPD->ENKDN": (0.5, 24.0),
}

# Product categories with distribution
PRODUCT_CATEGORIES = [
    ("Tracked24", 0.40),
    ("Tracked48", 0.40),
    ("SpecialDelivery09", 0.10),
    ("SpecialDelivery13", 0.10),
]

# Contact mix: (has_email, has_mobile, share)
CONTACT_MIX = [
    (False, False, 0.10),
    (True, False, 0.10),
    (False, True, 0.10),
    (True, True, 0.70),
]

# Account ratio
ACCOUNT_RATIO = 0.30  # 30% account (21-digit with embedded 10-digit account ID)

# Notification rules: (EventCode, ProductCategory) -> (EventPrefix, email_template_exists, sms_template_exists)
RULES = {
    ("EVDAV", "Tracked24"): ("NRA", True, False),
    ("EVGPD", "Tracked24"): ("NRB", True, True),
    ("ENKDN", "Tracked24"): ("NRC", True, False),
    ("EVGPD", "Tracked48"): ("NRE", True, True),
    ("ENKDN", "Tracked48"): ("NRF", False, True),
    ("EVDAV", "SpecialDelivery09"): ("NRG", True, True),
    ("EVGPD", "SpecialDelivery09"): ("NRH", True, True),
    ("ENKDN", "SpecialDelivery09"): ("NRI", False, True),
    ("EVDAV", "SpecialDelivery13"): ("NRJ", False, True),
    ("EVGPD", "SpecialDelivery13"): ("NRK", True, True),
    ("ENKDN", "SpecialDelivery13"): ("NRL", True, True),
}

# XML Namespaces and headers
INPUT_NS_HEADER = (
    '<ptp:MPE xmlns:dt="http://www.royalmailgroup.com/cm/rmDatatypes/V1" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:ptp="http://www.royalmailgroup.com/cm/ptpMailPiece/V1" '
    'xmlns="http://www.royalmailgroup.com/cm/ptpMailPiece/V1">'
)

OUTPUT_NS_HEADER = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ptp:MPE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:ptp="http://www.royalmailgroup.com/cm/ptpMailPiece/V1.3" '
    'xsi:schemaLocation="http://www.royalmailgroup.com/cm/ptpMailPiece/V1.3 ptpMailPiece.xsd">'
)

# Default Postcodes (91 real UK postcodes)
DEFAULT_POSTCODES = [
    "EC1A1BB",
    "W1A0AX",
    "M11AE",
    "B338TH",
    "UB70BH",
    "SW1A1AA",
    "E14BH",
    "G11AA",
    "BT71NN",
    "L16XX",
    "SE11ZZ",
    "N16XY",
    "NE10AA",
    "TN12AB",
    "GU11AA",
    "CF101BH",
    "EH12AB",
    "AB101NN",
    "KA11BB",
    "DD11AA",
    "SW1A0AA",
    "SW1A2AA",
    "WC2N5DU",
    "E16AN",
    "NW16XE",
    "W1J9HP",
    "SE17PB",
    "WC1E7HX",
    "E202ST",
    "TW62GA",
    "M25PD",
    "M160RA",
    "M113FF",
    "L39AG",
    "L40TH",
    "B11BB",
    "B66HE",
    "LS14AG",
    "LS110ES",
    "S12HH",
    "NE14ST",
    "NE18QP",
    "BS14UL",
    "BS81TH",
    "SO147FY",
    "PO13AX",
    "BN11EE",
    "PL12AA",
    "TR12EH",
    "EX11HS",
    "CB23QG",
    "OX12JD",
    "NR21RL",
    "IP11AE",
    "MK93EP",
    "RG12AG",
    "SL41NJ",
    "CT12EH",
    "YO17HH",
    "HU12AA",
    "G21DY",
    "G128QQ",
    "EH11RE",
    "EH89YL",
    "AB243FX",
    "G403RE",
    "G512XF",
    "DD14HN",
    "FK94LA",
    "IV35SS",
    "CF103NP",
    "SA13SN",
    "LL572DG",
    "SY233BY",
    "NP201GD",
    "LD16AA",
    "BT11AA",
    "BT95DL",
    "BT486SB",
    "BT601AA",
    "IM11AE",
    "GY11WG",
    "JE24WE",
    "TR210HE",
    "HS12AA",
    "KW14YT",
    "ZE10AX",
    "PA209NH",
    "KY169AJ",
    "PO301UD",
]

# UK Locations (95 cities/counties)
UK_LOCATIONS = [
    ("London", "Greater London"),
    ("Manchester", "Greater Manchester"),
    ("Birmingham", "West Midlands"),
    ("Leeds", "West Yorkshire"),
    ("Glasgow", "Glasgow City"),
    ("Liverpool", "Merseyside"),
    ("Bristol", "City of Bristol"),
    ("Sheffield", "South Yorkshire"),
    ("Newcastle upon Tyne", "Tyne and Wear"),
    ("Nottingham", "Nottinghamshire"),
    ("Leicester", "Leicestershire"),
    ("Edinburgh", "City of Edinburgh"),
    ("Cardiff", "South Glamorgan"),
    ("Belfast", "County Antrim"),
    ("Swansea", "West Glamorgan"),
    ("Plymouth", "Devon"),
    ("Southampton", "Hampshire"),
    ("Norwich", "Norfolk"),
    ("Exeter", "Devon"),
    ("Brighton", "East Sussex"),
    ("Oxford", "Oxfordshire"),
    ("Cambridge", "Cambridgeshire"),
    ("York", "North Yorkshire"),
    ("Milton Keynes", "Buckinghamshire"),
    ("Coventry", "West Midlands"),
    ("Stoke-on-Trent", "Staffordshire"),
    ("Reading", "Berkshire"),
    ("Preston", "Lancashire"),
    ("Bath", "Somerset"),
    ("Durham", "County Durham"),
    ("Portsmouth", "Hampshire"),
    ("Bournemouth", "Dorset"),
    ("Guildford", "Surrey"),
    ("Chelmsford", "Essex"),
    ("Maidstone", "Kent"),
    ("Luton", "Bedfordshire"),
    ("Swindon", "Wiltshire"),
    ("Cheltenham", "Gloucestershire"),
    ("Salisbury", "Wiltshire"),
    ("Canterbury", "Kent"),
    ("Truro", "Cornwall"),
    ("Poole", "Dorset"),
    ("Hastings", "East Sussex"),
    ("Ashford", "Kent"),
    ("Derby", "Derbyshire"),
    ("Wolverhampton", "West Midlands"),
    ("Telford", "Shropshire"),
    ("Northampton", "Northamptonshire"),
    ("Peterborough", "Cambridgeshire"),
    ("Ipswich", "Suffolk"),
    ("Lincoln", "Lincolnshire"),
    ("Worcester", "Worcestershire"),
    ("Shrewsbury", "Shropshire"),
    ("Grimsby", "Lincolnshire"),
    ("Hereford", "Herefordshire"),
    ("Bradford", "West Yorkshire"),
    ("Hull", "East Riding of Yorkshire"),
    ("Sunderland", "Tyne and Wear"),
    ("Middlesbrough", "North Yorkshire"),
    ("Wakefield", "West Yorkshire"),
    ("Blackpool", "Lancashire"),
    ("Bolton", "Greater Manchester"),
    ("Doncaster", "South Yorkshire"),
    ("Chester", "Cheshire"),
    ("Carlisle", "Cumbria"),
    ("Barrow-in-Furness", "Cumbria"),
    ("Rochdale", "Greater Manchester"),
    ("Stockport", "Greater Manchester"),
    ("Aberdeen", "Aberdeenshire"),
    ("Dundee", "Angus"),
    ("Inverness", "Highland"),
    ("Stirling", "Stirling"),
    ("Perth", "Perth and Kinross"),
    ("Falkirk", "Falkirk"),
    ("Oban", "Argyll and Bute"),
    ("St Andrews", "Fife"),
    ("Newport", "Gwent"),
    ("Wrexham", "Wrexham County Borough"),
    ("Bangor", "Gwynedd"),
    ("Carmarthen", "Carmarthenshire"),
    ("Aberystwyth", "Ceredigion"),
    ("Llandudno", "Conwy County Borough"),
    ("Haverfordwest", "Pembrokeshire"),
    ("Merthyr Tydfil", "Merthyr Tydfil County Borough"),
    ("Derry/Londonderry", "County Londonderry"),
    ("Lisburn", "County Antrim"),
    ("Newry", "County Down"),
    ("Armagh", "County Armagh"),
    ("Coleraine", "County Londonderry"),
    ("Enniskillen", "County Fermanagh"),
    ("Ballymena", "County Antrim"),
    ("St Helier", "Jersey"),
    ("Douglas", "Isle of Man"),
    ("St Peter Port", "Guernsey"),
]

# LMDB sub-DB names
DB_GLOBAL = b"global"
DB_SCAN1 = b"scan1"
DB_SCAN2 = b"scan2"
DB_SCAN3 = b"scan3"
DB_SCAN4 = b"scan4"
ALL_DBS = [DB_GLOBAL, DB_SCAN1, DB_SCAN2, DB_SCAN3, DB_SCAN4]


@dataclass
class Event:
    """Event data model"""

    parcel_id: str
    scan_no: int
    event_code: str
    event_time: datetime
    xml_value: bytes  # Packed: metadata + delimiter + xml
    metadata: dict  # Cached metadata for easy access
