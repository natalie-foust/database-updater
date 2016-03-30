import string, phpserialize, json, time
from urllib import urlencode
from urllib2 import urlopen
from databaseUpdater.utils import contains_whitespace, executeIfNotDebug, printIfVerbose

class MemberRow:
    def __init__(self, row, config):
        self.row                                =   row
        self.config                             =   config

    def previousLocatorDataExists(self, session):
        sql = """
SELECT * 
FROM ____________
WHERE
    PERSONOID       = {person_oid} AND
    SPECIALTYOID    = {specialty_oid} AND
    ORGANIZATIONOID = {organization_oid} AND
    ADDRESSOID      = {address_oid}
""".format( person_oid          =   self.person_oid,
            specialty_oid       =   self.specialty_oid,
            organization_oid    =   self.organization_oid,
            address_oid         =   self.address_oid)
        existing_data = session.execute(sql)
        existing_data = existing_data.fetchone()
        session.commit()
        if not existing_data:
            return False
        return existing_data

    def existingLocatorDataDiffers(self, session, existing_data):
        SQL_to_python = {"PERSONOID":"person_oid",
            "SPECIALTYOID":"specialty_oid",
            "PSPECIALTYOID":"practice_specialty_oid",
            "SPECIALTIES":"specialties",
            "ORGANIZATIONOID":"organization_oid",
            "ADDRESSOID":"address_oid",
            "location_name":"the_name",
            "phone":"phone",
            "url":"url",
            "zip":"zip",
            "_longitude":"longitude",
            "_latitude":"latitude"}
        for column_name in SQL_to_python.keys():
            new_column = SQL_to_python[column_name]
            if existing_data[column_name] != self.get(new_column):
                #TODO write verbose message explaining which row differs
                return True
        return False

    def prepareAllRowData(self, session):
        # Enter the default values into the object
        self._setInitialValues()

        # Get the gps information, if it is already in the locator_address_coord
        # table. If it is not, find it from google maps
        (self.longitude,
        self.latitude) = self._getOrGenerateGeocode(session)
        # Get the phone & URL information from our other tables. They both are
        # somewhat picky and strange in their formatting.
        self.phone = self._getAndFormatPhoneNumbers(session)
        self.url = self._getAndFormatURLs(session)
        self.specialties = self._getAndFormatSpecialties(session)
        self.hcsite = self._getSpecialtyChannel(session)
        self._fixStrings()

    def prepareJustGeoData(self, session):
        self._setInitialValues()

        # Get the gps information, if it is already in the locator_address_coord
        # table. If it is not, find it from google maps
        (self.longitude,
        self.latitude) = self._getOrGenerateGeocode(session)

    def updateGPSInformation(self, session):
        sql_insert_statement = self._generateInsertLocatorGPSStatement(session)
        executeIfNotDebug(session, sql_insert_statement, self.config)

    def insertRow(self, session):
        """
        Once all the information has been gathered, insert the data into the
        new ____________ table and pull a new member from the select query
        if possible.
        """
        sql_insert_statement = self._generateInsertStatement()
        executeIfNotDebug(session, sql_insert_statement, self.config)

    def updateExistingRow(self, session, existing_data):
        object_id = existing_data.object_id
        sql_update_statement = self._generateUpdateStatement(object_id)
        executeIfNotDebug(session, sql_update_statement, self.config)

    def _setInitialValues(self):
        """
        This method is run shortly after the row is created. It just re-formats
        the data to be more easily accessible.
        """
        row = self.row
        self.person_oid                         =   row.PERSONOID
        self.specialty_oid                      =   row.SPECIALTYOID
        self.secondary_specialty_oid            =   row.SECONDARYSPECIALTYOID
        self.organization_oid                   =   row.ORGANIZATIONOID
        self.the_name                           =   row.THENAME
        self.address_line_one                   =   row.ADDRESSLINEONE
        self.address_oid                        =   row.ADDRESSOID
        self.city                               =   row.CITY
        self.state                              =   row.STATE
        self.zip                                =   row.zip
        self.practice_specialty_oid             =   row.PSPECIALTYOID
        self.practice_primary_specialty_oid     =   row.PPRIMARYSPECIALTYOID
        self.practice_secondary_specialty_oid   =   row.PSECONDARYSPECIALTYOID
        self.member_status                      =   row.member_status
        self.role                               =   row.ROLE

    def _setInitialValuesForLocalSearch(self):
        row = self.row
        self.person_oid                         =   row.PERSONOID
        self.object_id                         =   row.object_id

    def _getOrGenerateGeocode(self, session):
        """ 
        This method returns the contents of the GEOCODE column on the
        to-be-created ____________ table. If the information is already establi-
        shed in the '_____________address_coord' table, it just grabs it from
        there, otherwise it attempts to create this information from the address
        info.
        """

        sql_statement ="""
SELECT * FROM _____________address_coord WHERE ADDRESSOID='{member_address}'
""".format(member_address=self.address_oid)
        address_gps_info = session.execute(sql_statement)
        session.commit()
        geocode = address_gps_info.fetchone()
        could_not_create_latitude_longitude = (None, None)
        if geocode is not None:
            self.geocode_already_set = True
        elif not self.address_line_one:
            self.geocode_already_set = False
            return could_not_create_latitude_longitude
        else:
            self.geocode_already_set = False

            if not self.address_line_one[0].isalnum():
                self.address_line_one = self.address_line_one[1:]

            url_parameters = { "sensor":"false" }
            if (self.city and self.state):
                url_parameters['address'] = ",".join([
                    self.address_line_one,
                    self.city,
                    self.state  ])
            elif self.zip:
                url_parameters['address'] = ",".join([
                    self.address_line_one,
                    self.zip ])
            else:
                return could_not_create_latitude_longitude
            json_uri = "{google_maps_json_uri}{encoded_address_city_state}".format(
                google_maps_json_uri="http://maps.google.com/maps/api/geocode/json?",
                encoded_address_city_state= urlencode(url_parameters))
            #TODO: account for URL errors
            try:
                json_response = urlopen(json_uri)
                json_data = json.loads(json_response.read())
                geocode = {}
                geocode["LONGITUDE"] = json_data["results"][0]["geometry"]["location"]["lng"]
                geocode["LATITUDE"] = json_data["results"][0]["geometry"]["location"]["lat"]
                
            except:
                return could_not_create_latitude_longitude
        return geocode["LONGITUDE"], geocode["LATITUDE"]        

    def _generateInsertLocatorGPSStatement(self, session):
        if (self.geocode_already_set or not self.address_line_one): return
        sql_statement ="""
INSERT
    _____________address_coord
SET 
    LONGITUDE='{longitude}',
    LATITUDE='{latitude}',
    date_update='{time}', 
    ADDRESSOID='{address_oid}'
""".format(
            longitude=self.longitude,
            latitude=self.latitude,
            time=int(time.time()),
            address_oid=self.address_oid    )
        return sql_statement


    def _getAndFormatPhoneNumbers(self, session):
        """
        Pull the phone information from the main database, and format it correc-
        tly for the ____________ table.
        """
        sql_statement ="""
SELECT  PhoneXOrganization.ROLE, Phone.COUNTRYCODE,
        Phone.AREACODE, Phone.THENUMBER
FROM    PhoneXOrganization LEFT JOIN Phone ON 
            PhoneXOrganization.PHONEOID=Phone.PHONEOID
WHERE   PhoneXOrganization.ORGANIZATIONOID='{organization_id}' AND 
        (PhoneXOrganization.ROLE='Main' OR PhoneXOrganization.ROLE='Toll Free')
""".format(organization_id=self.organization_oid)
        phones_results = session.execute(sql_statement)
        session.commit()
        phone = phones_results.fetchone()
        all_phones = ""
        while phone is not None:
            if phone.THENUMBER is None:
                phone = phones_results.fetchone()
                continue
            if all_phones:
                all_phones += "|"
            all_phones += "{role}@{country_code}{area_code}{number}".format(
                role=phone.ROLE,
                country_code=phone.COUNTRYCODE or "",
                area_code=phone.AREACODE or "",
                number=phone.THENUMBER
            )
            phone = phones_results.fetchone()
        return all_phones

    def _getAndFormatURLs(self, session):
        """
        Pull the URL information from the main database, and format it correctly
        for the ____________ table.
        """
        sql_statement = """
SELECT  OrganizationXHCURL.ROLE, HCURL.URL 
FROM    OrganizationXHCURL LEFT JOIN HCURL ON 
            OrganizationXHCURL.URLOID=HCURL.URLOID
WHERE   OrganizationXHCURL.ORGANIZATIONOID='{organization_id}' AND
        (OrganizationXHCURL.ROLE='Main' OR 
         OrganizationXHCURL.ROLE LIKE '%HC%' OR 
         OrganizationXHCURL.ROLE='Full' OR 
         OrganizationXHCURL.ROLE='Relative')
""".format(organization_id=self.organization_oid)
        url_results = session.execute(sql_statement)
        session.commit()
        current_url = url_results.fetchone()
        all_urls = {}
        while current_url is not None:
            #TODO could probably change this to a more comprehensive test of how
            # good the string is as a url, with little effort
            if (current_url.URL is None or contains_whitespace(current_url.URL)):
                current_url = url_results.fetchone()
                continue
            # TODO: This line is just copying the format from the php file, the
            # format makes no sense
            all_urls[current_url.ROLE] = dict(
                [(current_url.URL,current_url.URL)])

            current_url = url_results.fetchone()
        return phpserialize.dumps(all_urls)

    def _getAndFormatSpecialties(self, session):
        # Create and format the specialty_oid object
        specialties = {}
        specialty_columns = ["specialty_oid", "secondary_specialty_oid",
            "practice_specialty_oid", "practice_primary_specialty_oid",
            "practice_secondary_specialty_oid"]
        for specialty_column in specialty_columns:
            current_specialty_type = self.get(specialty_column)
            if not current_specialty_type:
                continue
            current_specialty_type = str(current_specialty_type)
            # More nonsense organization
            specialties[current_specialty_type] = current_specialty_type
        return specialties


    def _getSpecialtyChannel(self, session):
        sql_statement = """
SELECT
    HCChannelXSpecialty.SPECIALTYOID, HCChannel.BASE_URL
FROM 
    HCChannelXSpecialty LEFT JOIN HCChannel ON
        HCChannelXSpecialty.HCCHANNELOID=HCChannel.HCCHANNELOID
WHERE
    HCChannelXSpecialty.HCCHANNELOID!=8"""
        results = session.execute(sql_statement)
        session.commit()
        all_specialties_hc_channel = dict(results.fetchall())
        practice_specialty_long = long(self.practice_specialty_oid)
        if all_specialties_hc_channel.has_key(practice_specialty_long):
            return all_specialties_hc_channel[practice_specialty_long]
        else:
            return None

    def _fixStrings(self):
        self.the_name       =   self._fixStringIfExists(self.the_name)
        self.state          =   self._fixStringIfExists(self.state)
        self.specialties    =   "|{specialties}|".format(
            specialties = "|".join(self.specialties)    )
        self.phone          =   "|{phone}|".format(phone=self.phone)

    def _generateInsertStatement(self):
        sql_insert_statement = """
INSERT ____________ SET
    PERSONOID       =   '{person_oid}',
    SPECIALTYOID    =   '{specialty_oid}',
    PSPECIALTYOID   =   '{pspecialty_oid}',
    SPECIALTIES     =   '|{specialties}|',
    ORGANIZATIONOID =   '{organization_oid}',
    ADDRESSOID      =   '{address_oid}',
    location_name   =   '{location_name}',
    phone           =   '|{all_phones}|',
    url             =   '{url_data}',
    state           =   '{state}',
    zip             =   '{zipCode}',\
""".format(
            person_oid          =   self.person_oid,
            specialty_oid       =   self.specialty_oid,
            pspecialty_oid      =   self.practice_specialty_oid,
            specialties         =   self.specialties,
            organization_oid    =   self.organization_oid,
            address_oid         =   self.address_oid,
            location_name       =   self.the_name,
            all_phones          =   self.phone,
            url_data            =   self.url,
            state               =   self.state,
            zipCode             =   self.zip )
        if (self.longitude and self.latitude):
            sql_insert_statement +="""
    _longitude      =   '{geocode_longitude}', 
    _latitude       =   '{geocode_latitude}',""".format(
            geocode_longitude   =   self.longitude,
            geocode_latitude    =   self.latitude  )
        sql_insert_statement += """
    member_status   =   '{member_status}',
    hcsite          =   '{hcsite}';
""".format(
            member_status       =   "1",
            hcsite              =    self.hcsite)
        return sql_insert_statement

    def _generateUpdateStatement(self, object_id):
        sql_update_statement = """
UPDATE
    ____________
SET
    PERSONOID       =   '{person_oid}',
    SPECIALTYOID    =   '{specialty_oid}',
    PSPECIALTYOID   =   '{pspecialty_oid}',
    SPECIALTIES     =   '|{specialties}|',
    ORGANIZATIONOID =   '{organization_oid}',
    ADDRESSOID      =   '{address_oid}',
    location_name   =   '{location_name}',
    phone           =   '|{all_phones}|',
    url             =   '{url_data}',
    state           =   '{state}',
    zip             =   '{zipCode}',\
        """.format( person_oid          =   self.person_oid,
                    specialty_oid       =   self.specialty_oid,
                    pspecialty_oid      =   self.practice_specialty_oid,
                    specialties         =   self.specialties,
                    organization_oid    =   self.organization_oid,
                    address_oid         =   self.address_oid,
                    location_name       =   self.the_name,
                    all_phones          =   self.phone,
                    url_data            =   self.url,
                    state               =   self.state,
                    zipCode             =   self.zip )
        if (self.longitude and self.latitude):
            sql_update_statement +="""
    _longitude      =   '{geocode_longitude}', 
    _latitude       =   '{geocode_latitude}',""".format(
                geocode_longitude   =   self.longitude,
                geocode_latitude    =   self.latitude  )
        sql_update_statement += """
    member_status   =   '{member_status}',
    hcsite          =   '{hcsite}';
        """.format( member_status       =   "1",
                    hcsite              =    self.hcsite)
        sql_update_statement +="""
WHERE
    object_id      =   '{object_id}'""".format(object_id=object_id)

    def isRowMarkedForDeletion(self, session):
        self._setInitialValuesForLocalSearch()
        sql = """
SELECT
        REMOVE_FROM_DATABASE
FROM
        Practitioner
WHERE
        PERSONOID = {person_oid}
        """.format( person_oid= self.person_oid)
        results = session.execute(sql)
        session.commit()
        remove_from_database = bool(results.fetchone()[0])
        return remove_from_database

    def deleteRow(self, session):
        sql = """
DELETE FROM
    ____________
WHERE
    member_id = {member_id};
        """.format( object_id=self.object_id)
        if self.config.debug: print sql
        else:
            session.execute(sql)
            session.commit()

    def get(self, v):
        if not hasattr(self, v): return None
        return getattr(self, v)

    def _fixStringIfExists(self, s):
        if s is None: return None
        stripped = s.strip()
        fixed = stripped.replace("'","\\'")
        return fixed

    def isEmpty(self):
        """
        Returns True if the row is empty and False if there is still data.
        The row is empty when the select query has reached the end of the table.
        """
        if self.row is None: return True
        return False
