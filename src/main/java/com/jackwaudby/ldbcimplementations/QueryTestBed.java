package com.jackwaudby.ldbcimplementations;


import org.json.JSONObject;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Query test bed provides functionality for testing queries in a client-server model.
 */
public class QueryTestBed {
    public static void main(String[] args) {

        //
        // ["com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1",4398046512174,"Alexander",20]|
        // [
        //  [1367,"Budjana",2,395971200000,1265701747414,"female","Chrome","49.0.2.0",["Alexander1367@gmail.com"],["jv","en"],"Padang",
        //      [["Trunojoyo_University",2002,"Bangkalan"]],[["Indonesia_AirAsia",2003,"Indonesia"],["Jatayu_Airlines",2004,"Indonesia"]]],
        //         [2194,"Dobrunov",2,321408000000,1263666815626,"male","Chrome","31.31.175.171",["Alexander2194@gmail.com","Alexander2194@gmx.com"],["ru","en"],"Vologda",
        //         [["Siberian_State_Technological_University",2002,"Krasnoyarsk"]],[["Aviastar-TU",2002,"Russia"],["Samara_Airlines",2002,"Russia"],["KrasAir",2003,"Russia"],["2nd_Sverdlovsk_Air_Enterprise",2002,"Russia"]]],
        // [6597069766812,"Eduard",2,474076800000,1279713528949,"male","Internet Explorer","46.227.143.156",["Alexander6597069766812@gmail.com","Alexander6597069766812@gmx.com","Alexander6597069766812@zoho.com"],["uk","pl","en"],"Uzhhorod",[["Taras_Shevchenko_National_University_of_Kyiv",2004,"Kiev"]],[["Aerovis_Airlines",2005,"Ukraine"],["Ukraine_Air_Alliance",2004,"Ukraine"],["Aerosvit_Airlines",2004,"Ukraine"],["Challenge_Aero",2004,"Ukraine"]]],[2698,"Efimkin",2,477792000000,1263682994048,"male","Firefox","31.134.198.212",["Alexander2698@gmail.com","Alexander2698@hotmail.com","Alexander2698@tabouk.cc"],["ru","en"],"Izhevsk",[["Ural_State_Mining_University",2006,"Yekaterinburg"]],[["Dobrolet_Airlines",2007,"Russia"],["Omskavia",2006,"Russia"]]],[1965,"Gallagher",2,430617600000,1265003476715,"female","Chrome","27.131.249.85",["Alexander1965@hotmail.com","Alexander1965@intimatefire.com"],["nl","jv","en"],"Surabaya",[["Artha_Wacana_Christian_University",2005,"Kupang"]],[["Airfast_Indonesia",2005,"Indonesia"]]],[8796093023412,"Hleb",2,486950400000,1287556979057,"female","Internet Explorer","46.216.201.164",["Alexander8796093023412@congiu.net","Alexander8796093023412@gmail.com","Alexander8796093023412@yahoo.com"],["ru","uk","en"],"Barysaw",[],[["Gomelavia",2009,"Belarus"]]],[2214,"Ivanov",2,360460800000,1262415325582,"male","Internet Explorer","31.172.200.27",["Alexander2214@gmx.com","Alexander2214@yahoo.com"],["ru","en"],"Bishkek",[["Siberian_Federal_University",2003,"Krasnoyarsk"]],[["2nd_Sverdlovsk_Air_Enterprise",2004,"Russia"]]],[1662,"Kuzmina",2,599097600000,1265971359050,"male","Chrome","31.130.124.33",["Alexander1662@zoho.com"],["ru","en"],"Krasnoyarsk",[["Volgograd_State_University",2010,"Volgograd"]],[["Jet-2000",2011,"Russia"],["Aviast_Air",2011,"Russia"],["Abakan-Avia",2012,"Russia"],["VolaSalerno",2011,"Italy"]]],[800,"Popov",2,348278400000,1265945910023,"male","Chrome","31.28.2.140",["Alexander800@gmail.com","Alexander800@yahoo.com","Alexander800@zoho.com"],["ru","en"],"Kemerovo_Oblast",[["Magnitogorsk_State_University",2001,"Magnitogorsk"]],[["Air_Bashkortostan",2001,"Russia"],["Novosibirsk_Air_Enterprise",2001,"Russia"],["Globus_(airline)",2002,"Russia"],["Alrosa-Avia",2002,"Russia"]]],[6597069769360,"Popov",2,436752000000,1277541963153,"male","Firefox","31.3.22.98",["Alexander6597069769360@gmail.com"],["ru","en"],"Kazan",[["Siberian_State_Technological_University",2005,"Krasnoyarsk"]],[]],[2199023256358,"Aflalo",3,617328000000,1271195103370,"female","Chrome","81.192.85.153",["Alexander2199023256358@gmail.com","Alexander2199023256358@hotmail.com","Alexander2199023256358@tajikistan.cc","Alexander2199023256358@yahoo.com"],["ar","fr","en"],"Oujda",[],[["TransAVIAexport_Airlines",2010,"Belarus"],["Air_Arabia_Maroc",2010,"Morocco"],["Regional_Air_Lines",2008,"Morocco"],["Atlas_Blue",2009,"Morocco"],["Royal_Air_Maroc",2008,"Morocco"]]],[136,"Basov",3,380592000000,1264513851732,"male","Firefox","31.41.255.70",["Alexander136@gmail.com","Alexander136@yahoo.com"],["ru","en"],"Omsk",[["Siberian_Federal_University",2001,"Krasnoyarsk"]],[["Yamal_Airlines",2001,"Russia"],["Aviaenergo",2002,"Russia"],["Airstars",2002,"Russia"],["Elbrus-Avia",2001,"Russia"]]],[8796093022846,"Basov",3,438739200000,1286024363615,"male","Chrome","31.177.111.59",["Alexander8796093022846@gmail.com","Alexander8796093022846@gmx.com","Alexander8796093022846@yahoo.com","Alexander8796093022846@zoho.com"],["ru","en"],"Saint_Petersburg",[["Siberian_Federal_University",2001,"Krasnoyarsk"]],[]],[4398046513578,"Dobrunov",3,417312000000,1273923842752,"male","Safari","31.210.222.184",["Alexander4398046513578@gmail.com","Alexander4398046513578@gmx.com"],["ru","en"],"Bishkek",[],[["Red_Wings_Airlines",2005,"Russia"],["Orenair",2010,"Russia"],["Omskavia",2003,"Russia"]]],[6597069767972,"Eduard",3,484099200000,1278431006467,"male","Firefox","46.150.123.203",["Alexander6597069767972@gmx.com"],["uk","ro","en"],"Kharkiv",[],[["Air_Urga",2009,"Ukraine"],["UM_Airlines",2004,"Ukraine"],["Aerosvit_Airlines",2009,"Ukraine"],["Rivne_Universal_Avia",2010,"Ukraine"]]],[4398046513099,"Ivanov",3,350611200000,1276519900511,"male","Chrome","31.162.213.87",["Alexander4398046513099@gmail.com"],["ru","en"],"Saint_Petersburg",[],[["Saravia",2007,"Russia"],["Dagestan_Airlines",1999,"Russia"],["Airstars",2002,"Russia"]]],[10995116280291,"Ivanov",3,422150400000,1289690242508,"male","Chrome","31.181.251.145",["Alexander10995116280291@gmail.com","Alexander10995116280291@gmx.com","Alexander10995116280291@yahoo.com"],["ru","en"],"Yaroslavl",[["Saratov_State_Academy_of_Law",2005,"Saratov"]],[["Air_Bashkortostan",2006,"Russia"],["Aviastar-TU",2006,"Russia"],["Ak_Bars_Aero",2006,"Russia"],["ATRAN",2006,"Russia"]]],[2378,"Kahnovich",3,473817600000,1265401296691,"male","Chrome","31.3.21.52",["Alexander2378@gmail.com","Alexander2378@gmx.com","Alexander2378@hotmail.com"],["ru","en"],"Kursk",[["Magnitogorsk_State_University",2005,"Magnitogorsk"]],[["Nordwind_Airlines",2006,"Russia"],["ATRAN",2006,"Russia"],["Polet_Airlines",2005,"Russia"]]],[6597069769427,"Popov",3,436752000000,1280684917424,"male","Chrome","31.200.228.146",["Alexander6597069769427@gmail.com","Alexander6597069769427@yahoo.com","Alexander6597069769427@zoho.com"],["ru","en"],"Zhukovsky",[["Ural_State_Mining_University",2002,"Yekaterinburg"]],[["Alrosa_Mirny_Air_Enterprise",2002,"Russia"],["Air_Mauritanie",2003,"Mauritania"]]]]

        long personId = 4398046512174L;                       // person ID
        String personFirstName = "Alexander";             // person first name
        int limit = 20;


        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.withSack(0).V().has('Person','id',"+personId+")." +
                "repeat(both('knows').simplePath().sack(sum).by(constant(1))).emit().times(3)." +
                "dedup().has('Person','firstName','"+personFirstName+"')." +
                "order().by(sack(),asc).by('lastName',asc).by('id',asc).limit("+limit+")" +
                "local(union(" +
                "valueMap('lastName','id','email','birthday','creationDate','gender','browserUsed','locationIP','language'), " +
                "out('isLocatedIn').valueMap('name')," +
                "sack().fold(), " +
                "outE('workAt').as('workFrom').inV().as('company').out('isLocatedIn').as('country')." +
                "select('company','workFrom','country').by(valueMap('name')).by(valueMap()).fold()," +
                "outE('studyAt').as('studyFrom').inV().as('university').out('isLocatedIn').as('country')." +
                "select('university','studyFrom','country').by(valueMap('name')).by(valueMap()).fold()).fold())" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        String response = x.execute(queryString);
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);
        for (JSONObject result : results) {
            ArrayList<JSONObject> resultBreakdown = gremlinListToArrayList(result);
            HashMap<String,JSONObject> person = gremlinMapToHashMap(resultBreakdown.get(0));    // person result
            System.out.println(person);
            System.out.println(Long.parseLong(getPropertyValue(person.get("id"))));             // personId
            System.out.println(getPropertyValue(person.get("lastName")));
            System.out.println(Long.parseLong(getPropertyValue(person.get("birthday"))));     // birthday
            System.out.println(Long.parseLong(getPropertyValue(person.get("creationDate"))));   // creationDate
            System.out.println(getPropertyValue(person.get("gender")));                         // gender
            System.out.println(getPropertyValue(person.get("browserUsed")));                   // browser used
            System.out.println(getPropertyValue(person.get("locationIP")));
            int distanceFrom = Integer.parseInt(getPropertyValue(resultBreakdown.get(2)));      // distance result
            System.out.println(distanceFrom);
            String emails = getPropertyValue(person.get("email"));                              // email result
            List<String> emailList =
                    Arrays.asList(
                            emails.replaceAll("[\\[\\]\\s+]", "").split(","));
            if (emailList.size() == 1 && emailList.get(0).equals("")) {
                emailList = new ArrayList<>();
            }
            System.out.println(emailList);
            System.out.println();
            System.out.println(person.get("language"));
//            String speaks = getPropertyValue(person.get("speaks"));                             // speaks result
//            List<String> speaksList = Arrays.asList(
//                    speaks.replaceAll("[\\[\\]\\s+]", "").split(","));
//            System.out.println(speaksList);
        }
        x.onClose();
    }
}
