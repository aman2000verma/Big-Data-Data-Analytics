                         
Please be sure to refer to the "Important Notes" section below for essential 
information.

All daily data are calculated over the station's 24-hour LST day.

Field#  Name                           Units
---------------------------------------------
   1    WBANNO                         XXXXX
   2    LST_DATE                       YYYYMMDD
   3    CRX_VN                         XXXXXX
   4    LONGITUDE                      Decimal_degrees
   5    LATITUDE                       Decimal_degrees
   6    T_DAILY_MAX                    Celsius
   7    T_DAILY_MIN                    Celsius
   8    T_DAILY_MEAN                   Celsius
   9    T_DAILY_AVG                    Celsius
   10   P_DAILY_CALC                   mm
   11   SOLARAD_DAILY                  MJ/m^2
   12   SUR_TEMP_DAILY_TYPE            X
   13   SUR_TEMP_DAILY_MAX             Celsius
   14   SUR_TEMP_DAILY_MIN             Celsius
   15   SUR_TEMP_DAILY_AVG             Celsius
   16   RH_DAILY_MAX                   %
   17   RH_DAILY_MIN                   %
   18   RH_DAILY_AVG                   %
   19   SOIL_MOISTURE_5_DAILY          m^3/m^3
   20   SOIL_MOISTURE_10_DAILY         m^3/m^3
   21   SOIL_MOISTURE_20_DAILY         m^3/m^3
   22   SOIL_MOISTURE_50_DAILY         m^3/m^3
   23   SOIL_MOISTURE_100_DAILY        m^3/m^3
   24   SOIL_TEMP_5_DAILY              Celsius
   25   SOIL_TEMP_10_DAILY             Celsius
   26   SOIL_TEMP_20_DAILY             Celsius
   27   SOIL_TEMP_50_DAILY             Celsius
   28   SOIL_TEMP_100_DAILY            Celsius

   1    WBANNO  [5 chars]  cols 1 -- 5 
          The station WBAN number.

   2    LST_DATE  [8 chars]  cols 7 -- 14 
          The Local Standard Time (LST) date of the observation.

   3    CRX_VN  [6 chars]  cols 16 -- 21 
          The version number of the station datalogger program that was in 
          effect at the time of the observation. Note: This field should be 
          treated as text (i.e. string).

   4    LONGITUDE  [7 chars]  cols 23 -- 29 
          Station longitude, using WGS-84.

   5    LATITUDE  [7 chars]  cols 31 -- 37 
          Station latitude, using WGS-84.

   6    T_DAILY_MAX  [7 chars]  cols 39 -- 45 
          Maximum air temperature, in degrees C. See Note F.

   7    T_DAILY_MIN  [7 chars]  cols 47 -- 53 
          Minimum air temperature, in degrees C. See Note F.

   8    T_DAILY_MEAN  [7 chars]  cols 55 -- 61 
          Mean air temperature, in degrees C, calculated using the typical 
          historical approach: (T_DAILY_MAX + T_DAILY_MIN) / 2. See Note F.

   9    T_DAILY_AVG  [7 chars]  cols 63 -- 69 
          Average air temperature, in degrees C. See Note F.

   10   P_DAILY_CALC  [7 chars]  cols 71 -- 77 
          Total amount of precipitation, in mm. See Note F.

   11   SOLARAD_DAILY  [8 chars]  cols 79 -- 86 
          Total solar energy, in MJ/meter^2, calculated from the hourly average 
          global solar radiation rates and converted to energy by integrating 
          over time.

   12   SUR_TEMP_DAILY_TYPE  [1 chars]  cols 88 -- 88 
          Type of infrared surface temperature measurement. 'R' denotes raw 
          measurements, 'C' denotes corrected measurements, and 'U' indicates 
          unknown/missing. See Note G.

   13   SUR_TEMP_DAILY_MAX  [7 chars]  cols 90 -- 96 
          Maximum infrared surface temperature, in degrees C.

   14   SUR_TEMP_DAILY_MIN  [7 chars]  cols 98 -- 104 
          Minimum infrared surface temperature, in degrees C.

   15   SUR_TEMP_DAILY_AVG  [7 chars]  cols 106 -- 112 
          Average infrared surface temperature, in degrees C.

   16   RH_DAILY_MAX  [7 chars]  cols 114 -- 120 
          Maximum relative humidity, in %. See Notes H and I.

   17   RH_DAILY_MIN  [7 chars]  cols 122 -- 128 
          Minimum relative humidity, in %. See Notes H and I.

   18   RH_DAILY_AVG  [7 chars]  cols 130 -- 136 
          Average relative humidity, in %. See Notes H and I.

   19   SOIL_MOISTURE_5_DAILY  [7 chars]  cols 138 -- 144 
          Average soil moisture, in fractional volumetric water content (m^3/m^3), 
          at 5 cm below the surface. See Notes I and J.

   20   SOIL_MOISTURE_10_DAILY  [7 chars]  cols 146 -- 152 
          Average soil moisture, in fractional volumetric water content (m^3/m^3), 
          at 10 cm below the surface. See Notes I and J.

   21   SOIL_MOISTURE_20_DAILY  [7 chars]  cols 154 -- 160 
          Average soil moisture, in fractional volumetric water content (m^3/m^3), 
          at 20 cm below the surface. See Notes I and J.

   22   SOIL_MOISTURE_50_DAILY  [7 chars]  cols 162 -- 168 
          Average soil moisture, in fractional volumetric water content (m^3/m^3), 
          at 50 cm below the surface. See Notes I and J.

   23   SOIL_MOISTURE_100_DAILY  [7 chars]  cols 170 -- 176 
          Average soil moisture, in fractional volumetric water content (m^3/m^3), 
          at 100 cm below the surface. See Notes I and J.

   24   SOIL_TEMP_5_DAILY  [7 chars]  cols 178 -- 184 
          Average soil temperature, in degrees C, at 5 cm below the surface. 
          See Notes I and J.

   25   SOIL_TEMP_10_DAILY  [7 chars]  cols 186 -- 192 
          Average soil temperature, in degrees C, at 10 cm below the surface. 
          See Notes I and J.

   26   SOIL_TEMP_20_DAILY  [7 chars]  cols 194 -- 200 
          Average soil temperature, in degrees C, at 20 cm below the surface. 
          See Notes I and J.

   27   SOIL_TEMP_50_DAILY  [7 chars]  cols 202 -- 208 
          Average soil temperature, in degrees C, at 50 cm below the surface. 
          See Notes I and J.

   28   SOIL_TEMP_100_DAILY  [7 chars]  cols 210 -- 216 
          Average soil temperature, in degrees C, at 100 cm below the surface. 
          See Notes I and J.

    IMPORTANT NOTES:
        A.  All fields are separated from adjacent fields by at least one space.
        B.  Leading zeros are omitted.
        C.  Missing data are indicated by the lowest possible integer for a 
            given column format, such as -9999.0 for 7-character fields with 
            one decimal place or -99.000 for 7-character fields with three
            decimal places.
        D.  Daily data are calculated using the station's local day. 
        E.  There are no quality flags for these derived quantities. When the 
            raw data are flagged as erroneous, these derived values are not 
            calculated, and are instead reported as missing. Therefore, these 
            fields may be assumed to always be good (unflagged) data, except 
            when they are reported as missing.
        F.  The daily values reported in this dataset are calculated using 
            multiple independent measurements for temperature and precipitation. 
            USCRN/USRCRN stations have multiple co-located temperature sensors 
            that make 10-second independent measurements which are used to 
            produce max/min/avg temperature values at 5-minute intervals. The
            precipitation gauge is equipped with multiple load cell sensors to 
            provide independent measurements of depth change at 5-minute 
            intervals. 
        G.  On 2013-01-07 at 1500 UTC, USCRN began reporting corrected surface 
            temperature measurements for some stations. These changes  
            impact previous users of the data because the corrected values 
            differ from uncorrected values. To distinguish between uncorrected 
            (raw) and corrected surface temperature measurements, a surface 
            temperature type field was added to the daily01 product. The 
            possible values of the this field are "R" to denote raw surface 
            temperature measurements, "C" to denote corrected surface 
            temperature measurements, and "U" for unknown/missing.
        H.  Relative humidity is computed from 5-minute values in almost all 
            cases. All USCRN stations now report 5-minute averages, however the 
            two Asheville, NC stations reported only hourly RH values until 
            2007-02-22.
        I.  USRCRN stations do not measure solar radiation, surface temperature,
            relative humidity or soil variables, so those fields are shown as 
            missing data.
        J.  USCRN stations have multiple co-located soil sensors that record 
            independent measurements. The soil values reported in this dataset 
            are an average of the day's hourly soil measurements which are 
            calculated from the multiple independent measurements. Soil 
            moisture is the ratio of water volume over sample volume 
            (m^3 water/m^3 soil).
        K.  In accordance with Service Change Notice 14-25 from the National 
            Weather Service, NCDC stopped providing data from the 72 
            Southwest Regional Climate Reference Network (USRCRN) stations on 
            June 1, 2014. The historical data for these stations remain 
            available. 