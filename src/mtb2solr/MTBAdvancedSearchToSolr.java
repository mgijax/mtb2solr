package mtb2solr;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import org.jax.mgi.mtb.dao.custom.SearchResults;
import org.jax.mgi.mtb.dao.custom.mtb.MTBAdvancedSearchDAO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.mtb.dao.custom.mtb.MTBStrainTumorSummaryDTO;
import org.jax.mgi.mtb.dao.mtb.DAOManagerMTB;

import org.apache.log4j.*;

/**
 *
 * runs as a mtbadmin cron job on bhmtbdb01 2 AM Sundays
 *
 * @author sbn
 *
 * java -cp ./mtb2solr.jar mtb2solr.MTBAdvancedSearchToSolr
 *
 * solr notes: schema.xml
 * /usr/local/mgi/mtb/solr/solr-5.1.0/server/solr/mtb3/conf
 *
 * test2.html /usr/local/mgi/mtb/solr/solr-5.1.0/server/solr-webapp/webapp
 */
public class MTBAdvancedSearchToSolr {
    
    // there is some reason to use strings

   
    private HashMap<String, String[]> organParentMap = new HashMap();
    private HashMap<String, String> tumorClassificationParentMap = new HashMap();
    private HashMap<String, ArrayList<String>> tumorMarkerMap = new HashMap();
    private HashMap<String, ArrayList<String>> strainMarkerMap = new HashMap();
    private HashMap<String, ArrayList<String>> markerSynonymMap = new HashMap();
    private HashMap<String, ArrayList<String>> strainGenotypeMap = new HashMap();
    private HashMap<String, ArrayList<String>> strainAlleleMap = new HashMap();
    private HashMap<String, ArrayList<String>> strainNameMap = new HashMap();
    private HashMap<String, String> strainFamilyKeyMap = new HashMap();
    private HashMap<String, String> abstractMap = new HashMap();
    private HashMap<String, ArrayList<String>> authorsMap = new HashMap();
    private HashMap<String, Integer> yearMap = new HashMap();
    private HashMap<String, String> titleMap = new HashMap();
    private HashMap<String, String> journalMap = new HashMap();
    private HashMap<String, String> citationMap = new HashMap();
     
    private HashMap<String, String> referenceIDMap = new HashMap();
    private HashMap<String, String> mutantsMap = new HashMap();
    private HashMap<String, String> humanTissueMap = new HashMap();
    private HashMap<String, String> humanTissueNameMap = new HashMap();
    private HashMap<String, String> excludeTumorClassificationForModels = new HashMap();
    private HashMap<Integer, String> singleMutantModels = new HashMap();
    private HashMap<Integer, ArrayList<String>> strainMutationTypeMap = new HashMap();
    private HashMap<String, ArrayList<String>> tumorMutationTypeMap = new HashMap();
    static Logger logger = Logger.getLogger(MTBAdvancedSearchToSolr.class);
    private static int minFreqNum = 80;
    private static int minColonySize = 20;
    private HashMap<String, String> minFreqColony = new HashMap();
    private HashMap<String, ArrayList<String>> seriesMap = new HashMap();
    private static Properties props = new Properties();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        
        Logger.getRootLogger().setLevel(Level.ERROR);
        
        try{
            props.load(MTBAdvancedSearchToSolr.class.getResourceAsStream("mtb2solr.properties")); 
           
        }catch(Exception e){
            System.out.println("Unable to load properties exiting");
            e.printStackTrace();
            System.exit(0);
        }
        BasicConfigurator.configure();

        MTBAdvancedSearchToSolr m2s = new MTBAdvancedSearchToSolr();
                
        Collection<SolrInputDocument> docs = m2s.buildDocs();
                
  
          Indexer idx = Indexer.getInstance(props.getProperty("solr_url"));

        if (docs != null && docs.size() > 40000) {

       //     System.out.println("Deleting existing docs");

       //     idx.delete();

            System.out.println("Sending " + docs.size() + " docs to Indexer for " + props.getProperty("solr_url"));

       //     idx.writeDocs(docs);
        } else {
            System.out.print("Quitting. No changes made to solr. Only " + docs.size() + " docs generated from MTB ");

        }

        System.out.println("Done.");

    }

    private Collection<SolrInputDocument> buildDocs() {
        
      
        ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

        try {

            connectMTB();

            // create a map from organ to parent organ
            loadParentMaps();
            
            loadMarkerSynonyms();

            // nixed!    loadTumorMarkers();
            
            loadStrainMarkers();
            
            loadStrainAlleles();
            
            loadStrainGenotypes();
            
            loadStrainFamilyKeys();
            
            loadStrainNames();

            loadPubMedIDs();
            
            loadReferences();

            loadMinFreqColony();

            loadSeries();

            loadMutants();

            loadExcludeTumorClassificationForModels();

            loadHumanTissuesMap();
            
       //     loadSingleMutantModels();
       
            loadTumorMutationTypes();
       
            loadStrainMutationTypes();

            String strSortBy = "tumorclassification";
            int nMaxItems = -1;
            int nAgentType = -1;

            List<String> colStrainTypes = new ArrayList<String>();

            List<String> colOrgansOrigin = new ArrayList<String>();

            List<String> colTumorClassifications = new ArrayList<String>();

            MTBAdvancedSearchDAO daoAdvanced = MTBAdvancedSearchDAO.getInstance();

            // create the collection of matching tumor to return
            SearchResults res = null;

            res = daoAdvanced.search(colOrgansOrigin,
                    colTumorClassifications,
                    nAgentType,
                    false,
                    false,
                    "",
                    "",
                    colStrainTypes,
                    "",
                    strSortBy,
                    nMaxItems, -1, -1);

            int id = 0;
            
            ArrayList<MTBStrainTumorSummaryDTO> list = (ArrayList< MTBStrainTumorSummaryDTO>) res.getList();
            System.out.println("Loaded " + list.size() + " tumor records from MTB");
            System.out.println("Summarizing " + res.getAncillaryTotal() + " total TF records");

            for (MTBStrainTumorSummaryDTO tumor : list) {
                SolrInputDocument doc = new SolrInputDocument();

                doc.addField("id", id++);
                String tumorName = tumor.getTumorName();

                String oo = tumorName.substring(0, (tumorName.indexOf(tumor.getTumorClassName())));

                String tClassification = tumor.getTumorClassName();
                
                doc.addField("tumorClassification", tClassification);
                doc.addField("strain", tumor.getStrainName());
                doc.addField("strainKey", tumor.getStrainKey());
                doc.addField("strainFamilyKey", strainFamilyKeyMap.get(tumor.getStrainKey()));
           
                
                String[] treatments = tumor.getTreatmentType().split(",");
                for(String treatment : treatments){
                    doc.addField("agentType",treatment.trim());
                }
                doc.addField("tcParent", tumorClassificationParentMap.get(tumor.getTumorClassName()));;
                doc.addField("organAffected", tumor.getOrganAffectedName());
                doc.addField("organOrigin", oo.trim());
                doc.addField("organOriginKey", tumor.getOrganOfOriginKey());
                doc.addField("organOriginParent", organParentMap.get(tumor.getOrganOfOriginKey() + "")[0]);
                doc.addField("organOriginParentKey", organParentMap.get(tumor.getOrganOfOriginKey() + "")[1]);
                
                
                
                
                try{
                    
                    doc.addField("strainNames", deHTML(tumor.getStrainName()));
                    if(strainNameMap.containsKey(new Integer(tumor.getStrainKey()))){
                        for(String name : strainNameMap.get(tumor.getStrainKey())){
                            if(!deHTML(name).equals(tumor.getStrainName())){
                                addUniqueField(doc,"strainNames",deHTML(name)+" ("+tumor.getStrainName()+")");
                            }else{
                                // name is same as offical name
                                addUniqueField(doc,"strainNames",deHTML(name));
                            }
               
                            
                        }
                    }
                }catch(Exception e){}
                

                try {

                    for (String ref : tumor.getSortedRefAccIds()) {
                        if (referenceIDMap.get(ref) != null) {
                            doc.addField("referenceID", referenceIDMap.get(ref));
                            
                        } else {
                            doc.addField("referenceID", ref);
                        }
                        if(authorsMap.containsKey(ref)){
                            for(String author : authorsMap.get(ref)){
                                doc.addField("authors",author);
                            }
                            doc.addField("journal", journalMap.get(ref));
                            doc.addField("year", yearMap.get(ref));
                            doc.addField("citation",citationMap.get(ref));
                            doc.addField("title", titleMap.get(ref));
                            doc.addField("abstract",abstractMap.get(ref));
                            
                            
                                    
                        }
                    }

                } catch (Exception e) {
                }

                try {

                    for (String type : tumor.getStrainTypesCollection()) {
                        doc.addField("strainType", type);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (strainMarkerMap.containsKey(tumor.getStrainKey() + "")) {
                    for (String marker : strainMarkerMap.get(tumor.getStrainKey() + "")) {
                        doc.addField("strainMarker", marker);
                    }
                }
                
                // ideally these go into a seperate field then are combined in solr for the genes and alleles facet
                // but not now, not now.
                if (strainAlleleMap.containsKey(tumor.getStrainKey() + "")) {
                    for (String allele : strainAlleleMap.get(tumor.getStrainKey() + "")) {
                        // for now keep the HTML markup in the alleles
                        doc.addField("strainMarker", allele);
                        
                    }
                }
                
                if (strainGenotypeMap.containsKey(tumor.getStrainKey())) {
                    for (String genotype : strainGenotypeMap.get(tumor.getStrainKey())) {
                        addUniqueField(doc,"strainGenotype", genotype);
                        addUniqueField(doc,"strainGenotypeNoHTML", deHTML(genotype));
                        //System.out.println("added genotype "+genotype);
                    }
                }

                if (mutantsMap.containsKey(tumor.getStrainKey() + "")) {
                    doc.addField("mutant", true);
                }
                
                if(singleMutantModels.containsKey(tumor.getStrainKey()+"")){
                    doc.addField("singleMutant", true);
                   
          //          System.out.println("single mutant # "+ smCount);
                }
                
                if(strainMutationTypeMap.containsKey(tumor.getStrainKey())){
                    for(String mutationType : strainMutationTypeMap.get(tumor.getStrainKey())){
                        addUniqueField(doc,"strainMutationType", mutationType);
                    }
                }
               

                if (!excludeTumorClassificationForModels.containsKey(tumor.getTumorClassificationKey() + "")) {
                    if (humanTissueMap.containsKey(tumor.getOrganOfOriginKey() + "")) {
                        addUniqueField(doc,"humanTissue", humanTissueMap.get(tumor.getOrganOfOriginKey() + ""));
                        //        System.out.println("Got humanTissue: "+humanTissueMap.get(tumor.getOrganOfOriginKey()+"")+" for organ origin key "+tumor.getOrganOfOriginKey());
                    }
                    // can't get parent organ key..... BUG! "(Unspecified Organ)" can be for multiple tissues currently always mapped to "soft tissue" BUG! 
                    if (humanTissueNameMap.containsKey(tumor.getOrganAffectedName()) && !tumor.getOrganAffectedName().contains("Unspecified organ")) {
                        addUniqueField(doc,"humanTissue", humanTissueNameMap.get(tumor.getOrganAffectedName() + ""));
                        //          System.out.println("Got humanTissue: "+humanTissueNameMap.get(tumor.getOrganAffectedName()+"")+" for organ affected "+tumor.getOrganAffectedName());
                    }

                } else {
                   // System.out.println("excluding TC KEY" + tumor.getTumorClassificationKey());
                }

                try {
                    boolean metastatic = false;
                    for (String mets : tumor.getMetastasizesToDisplay()) {
                        doc.addField("metsTo", mets);
                        if (!mets.startsWith("<i>not")) {
                            metastatic = true;
                        }
                    }
                    doc.addField("metastatic", metastatic);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {

                    for (String agent : tumor.getAgentsCollection()) {
                        doc.addField("agent", agent);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {

                    for (String tfKey : tumor.getAllTFKeys()) {
                        doc.addField("tumorFrequencyKey", tfKey);

                        if (tumorMarkerMap.containsKey(tfKey)) {
                            for (String marker : tumorMarkerMap.get(tfKey)) {
                                doc.addField("tumorMarker", marker);
                            }
                        }
                        
                        // um maybe we want this to be set to 1 if its true for all tfkeys in the tumor summary dto
                        if (minFreqColony.containsKey(tfKey)) {
                            doc.setField("minFC", "1");
                           
                        }
                        /*                  
                        if(seriesMap.containsKey(tfKey)){
                            for(String series: seriesMap.get(tfKey)){
                                doc.addField("series",series);
                            }
                             doc.addField("geneExpression", "true");
                        }
                         */
                        
                        
                         if(tumorMutationTypeMap.containsKey(tfKey)){
                            for(String mutationType : tumorMutationTypeMap.get(tfKey)){
                                addUniqueField(doc,"tumorMutationType", mutationType);
                            }
                         }
                

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                doc.addField("freqM", tumor.getFreqMaleString());
                doc.addField("freqF", tumor.getFreqFemaleString());
                doc.addField("freqU", tumor.getFreqUnknownString());
                doc.addField("freqX", tumor.getFreqMixedString());
                doc.addField("freqAll", tumor.getFreqAllString());
                
                doc.addField("freqNumM", maxFreqsToNum(tumor.getFreqMale()));
                doc.addField("freqNumF", maxFreqsToNum(tumor.getFreqFemale()));
                doc.addField("freqNumU", maxFreqsToNum(tumor.getFreqUnknown()));
                doc.addField("freqNumX", maxFreqsToNum(tumor.getFreqMixed()));
                
               
                
                
//                System.out.println(tumor.getFreqMaleString()+" "+ maxFreqsToNum(tumor.getFreqMale()));
//                System.out.println(tumor.getFreqFemaleString()+" "+ maxFreqsToNum(tumor.getFreqFemale()));
//                System.out.println(tumor.getFreqUnknownString()+" "+ maxFreqsToNum(tumor.getFreqUnknown()));
//                System.out.println(tumor.getFreqMixedString()+" "+ maxFreqsToNum(tumor.getFreqMixed()));
//                System.out.println(tumor.getFreqAllString());
//                System.out.println();System.out.println();

                
                docs.add(doc);

            }

            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            PreparedStatement ps = conn.prepareStatement(query.toString());

            int docCount = 0;
            for (SolrInputDocument doc : docs) {
                docCount++;

                if (docCount % 50 == 0) {
                    System.out.println("Loaded extra data for " + docCount + " records.");
                }
                
                if (docCount % 2000 == 0) {
                    System.out.println("Resetting postgres connection to prevent timeout");
                    conn.close();
                    conn = manager.getConnection();
                }

                StringBuilder tfks = new StringBuilder("(");

                for (Object tfk : doc.getFieldValues("tumorFrequencyKey")) {
                    if (tfks.length() > 1) {
                        tfks.append(",");
                    }
                    tfks.append(tfk);
                }
                tfks.append(")");

                query = new StringBuffer("select count(i._Images_key) ");
                query.append(" from Images i, PathologyImages pi, TumorPathologyAssoc tpa where tpa._Pathology_key = pi._Pathology_key ");
                query.append(" and pi._images_key = i._images_key and i.privateFlag != 1 ");
                query.append(" and tpa._TumorFrequency_key in  ").append(tfks.toString());
                ps = conn.prepareStatement(query.toString());

                rs = ps.executeQuery();
                while (rs.next()) {
                    try {
                        if (rs.getInt(1) > 0) {
                            doc.addField("pathologyImages", rs.getInt(1));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                rs.close();
                ps.close();

                query = new StringBuffer("select tgc._TumorFrequency_key, count(ai._assayimages_key)  from AssayImages ai, TmrGntcCngAssayImageAssoc ass, TumorGeneticChanges tgc ");
                query.append(" where tgc._TumorGeneticChanges_key = ass._TumorGeneticChanges_key and ai._assayimages_key = ass._assayimages_key ");
                query.append(" and ai.privateFlag !=1 ");
                query.append(" and tgc._TumorFrequency_key in  ").append(tfks.toString());
                query.append(" group by tgc._TumorFrequency_key");

                ps = conn.prepareStatement(query.toString());

                rs = ps.executeQuery();
                StringBuilder cytoKeys = new StringBuilder();
                int count = 0;
                while (rs.next()) {
                    try {
                        if (rs.getInt(2) > 0) {
                            count += rs.getInt(2);
                            if (cytoKeys.length() > 0) {
                                cytoKeys.append(",");
                            }
                            cytoKeys.append(rs.getInt(1));
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (cytoKeys.length() > 0) {
                    doc.addField("cytoImages", cytoKeys.toString());
                    doc.addField("cytoCount", count);
                }

                rs.close();
                ps.close();

                query = new StringBuffer("select count(s.id) from Sample s, SampleAssoc sa  where sa._sample_key = s._sample_key  ");
                query.append(" and  sa._MTBTypes_key = 5 and sa._object_key in  ").append(tfks.toString());

                ps = conn.prepareStatement(query.toString());

                rs = ps.executeQuery();
                while (rs.next()) {
                    try {
                        if (rs.getInt(1) > 0) {
                            doc.addField("geneExpression", "true");

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                rs.close();
                ps.close();

                query = new StringBuffer("select max(freqNum),min(freqNum) from TumorFrequency  ");
                query.append(" where _tumorfrequency_key not in (select _child_key from TumorProgression) and  _tumorfrequency_key in  ").append(tfks.toString());

                ps = conn.prepareStatement(query.toString());

                rs = ps.executeQuery();
                while (rs.next()) {
                    doc.addField("freqMax", rs.getFloat(1));
                    doc.addField("freqMin", rs.getFloat(2));
                }

                rs.close();
                ps.close();
                
                
                
                query = new StringBuffer("select max(colonySize),min(colonySize) from TumorFrequency  ");
                query.append(" where  _tumorfrequency_key in  ").append(tfks.toString());

                ps = conn.prepareStatement(query.toString());

                rs = ps.executeQuery();
                while (rs.next()) {
                    doc.addField("colonyMax", rs.getFloat(1));
                    
                    doc.addField("colonyMin", rs.getFloat(2));
                    
                //    System.out.println(rs.getInt(1)+" "+rs.getInt(2)+ " "+tfks.toString());
                }

                rs.close();
                ps.close();

            }
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return docs;
    }

    private void loadTumorMarkers() {
        String sql = "Select distinct symbol, tfKey from "
                + "(SELECT m.symbol as symbol, tg._tumorfrequency_key as tfKey  from  Marker m,  AllelePair ap, TumorGenetics tg, AlleleMarkerAssoc ama "
                + "where m._marker_key = ama._marker_key and "
                + "ama._allele_key = ap._allele1_key "
                + "and ap. _allelePair_key = tg._allelePair_key "
                + "union "
                + "SELECT m.symbol as symbol , tg._tumorfrequency_key as tfKey from  Marker m,  AllelePair ap, TumorGenetics tg, AlleleMarkerAssoc ama "
                + "where m._marker_key = ama._marker_key and "
                + "ama._allele_key = ap._allele2_key "
                + "and ap. _allelePair_key = tg._allelePair_key) as uni";

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Tumor Markers");
            rs = ps.executeQuery();
            while (rs.next()) {
                if (tumorMarkerMap.containsKey(rs.getInt(2) + "")) {
                    tumorMarkerMap.get(rs.getInt(2) + "").add(rs.getString(1));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(1));
                    tumorMarkerMap.put(rs.getInt(2) + "", list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void loadStrainMarkers() {
        
        
        
        // this is called germline mutant alleles in the faceted search
        // we should exclude? allele types 22,30 like we do for mutant strains?
         String sql = "Select distinct symbol, name, mKey, sKey from "
                + "(SELECT m.symbol as symbol, m.name as name, m._marker_key as mKey,  g._strain_key as sKey  from  Marker m,  AllelePair ap, Genotype g, AlleleMarkerAssoc ama, Allele a "
                + "where m._marker_key = ama._marker_key and "
                + "ama._allele_key = ap._allele1_key "
                + "and ap. _allelePair_key = g._allelePair_key and ama._allele_key = a._allele_key and a._alleletype_key not in(22,30) "
                + "union "
                + "SELECT m.symbol as symbol, m.name as name, m._marker_key as mKey, g._strain_key as sKey from  Marker m,  AllelePair ap, Genotype g, AlleleMarkerAssoc ama, Allele a "
                + "where m._marker_key = ama._marker_key and "
                + "ama._allele_key = ap._allele2_key "
                + "and ap. _allelePair_key = g._allelePair_key and ama._allele_key = a._allele_key and a._alleletype_key not in(22,30)) as uni";

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Markers");
            rs = ps.executeQuery();
            while (rs.next()) {
                if (strainMarkerMap.containsKey(rs.getInt(4) + "")) {
                    strainMarkerMap.get(rs.getInt(4) + "").add(rs.getString(1));
                    strainMarkerMap.get(rs.getInt(4) + "").add(rs.getString(2));
                    
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(1));
                    list.add(rs.getString(2));
                    strainMarkerMap.put(rs.getInt(4) + "", list);
                }
                
                if(markerSynonymMap.containsKey(rs.getInt(3)+"")){
                        for(String syn : markerSynonymMap.get(rs.getInt(3)+"")){
                            strainMarkerMap.get(rs.getInt(4) + "").add(syn);
                  //          System.out.println("added marker synonym "+syn);
                        }
                    }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void loadMarkerSynonyms() {
        
        // this is called germline mutant alleles in the faceted search
        // we should exclude? allele types 22,30 like we do for mutant strains?
        String sql = "select concat(ml.label,' (', m.name,')'), m._marker_key from markerlabel ml, marker m where ml._labeltype_key='MY' and ml._marker_key = m._marker_key;";

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Marker Synonyms");
            rs = ps.executeQuery();
            while (rs.next()) {
                if (markerSynonymMap.containsKey(rs.getInt(2) + "")) {
                    markerSynonymMap.get(rs.getInt(2) + "").add(rs.getString(1));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(1));
                    markerSynonymMap.put(rs.getInt(2) + "", list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void loadStrainMutationTypes(){
    StringBuilder sql= new StringBuilder("select sub.strainKey, sub.alleleType from (");
                  sql.append("select g._strain_key as strainKey, at.type as alleleType from ");
                  sql.append("genotype g, allelepair ap, allele a, alleletype at ");
                  sql.append(" where g._allelepair_key = ap._allelepair_key and ");
                  sql.append(" ap._allele1_key = a._allele_key and a._alleletype_key = at._alleletype_key ");
                  sql.append(" union ");
                  sql.append("select g._strain_key as strainKey, at.type as alleleType from ");
                  sql.append("genotype g, allelepair ap, allele a, alleletype at ");
                  sql.append(" where g._allelepair_key = ap._allelepair_key and ");
                  sql.append(" ap._allele2_key = a._allele_key and a._alleletype_key = at._alleletype_key) as sub ");
    
     try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql.toString());

            System.out.println("Loading strain mutation types");
            rs = ps.executeQuery();
            
            while (rs.next()) {
                while (rs.next()) {
                if (strainMutationTypeMap.containsKey(rs.getInt(1))) {
                    strainMutationTypeMap.get(rs.getInt(1)).add(rs.getString(2));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(2));
                    strainMutationTypeMap.put(rs.getInt(1), list);
                }

            }

            }

            rs.close();
            ps.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
     private void loadStrainAlleles() {
        
        // this is called germline mutant alleles in the faceted search
        // we should exclude? allele types 22,30 like we do for mutant strains?
        
	 String sql = "SELECT distinct strainKey, symbol from ( "
		+ " SELECT g._strain_key as strainKey, a.symbol as symbol  from Allele a, Genotype g, AllelePair ap "
                + "WHERE ap._allelePair_key = g._allelePair_key and ap._allele1_key = a._allele_key "
                + " and a._alleletype_key not in(22,30)"
		+ " union "
		+ "SELECT g._strain_key as strainKey, a.symbol as symbol  from Allele a, Genotype g, AllelePair ap "
                + "WHERE ap._allelePair_key = g._allelePair_key and ap._allele2_key = a._allele_key "
                + " and a._alleletype_key not in(22,30)) as uni";
               
                   
        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Alleles");
            rs = ps.executeQuery();
            String key = "0";
            while (rs.next()) {
                key = rs.getInt(1)+"";
                if (strainAlleleMap.containsKey(key)) {
                    strainAlleleMap.get(key).add(rs.getString(2));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(2));
                    strainAlleleMap.put(key, list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
     private void loadStrainGenotypes() {
        
        // this is called germline mutant alleles in the faceted search
        // we should exclude? allele types 22,30 like we do for mutant strains?
        String sql = "SELECT g._strain_key, concat(a1.symbol,'/',a2.symbol)  from Allele a1, Genotype g, AllelePair ap left outer join Allele a2 on ( ap._allele2_key = a2._allele_key  and a2._alleletype_key not in(22,30) )"
                + "WHERE ap._allelePair_key = g._allelePair_key and ap._allele1_key = a1._allele_key "
                + " and a1._alleletype_key not in(22,30)";
        
	        
                   
        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Genotypes");
            rs = ps.executeQuery();
            String key = "0";
            while (rs.next()) {
                key = rs.getInt(1)+"";
                if (strainGenotypeMap.containsKey(key)) {
                    strainGenotypeMap.get(key).add(rs.getString(2));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(2));
                    strainGenotypeMap.put(key, list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    
     private void loadStrainNames() {
        
         
        String sql =  "SELECT name, _strain_key from strainsynonyms ";

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Names");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                
                if (strainNameMap.containsKey(rs.getInt(2)+"")) {
                    strainNameMap.get(rs.getInt(2)+"").add(name);
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(name);
                    strainNameMap.put(rs.getInt(2)+"", list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     
     
     
     private void loadStrainSynonyms() {
     
     
        String sql =  "SELECT name, _strain_key from strainsynonyms ";
     
        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Names");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                
                if (strainNameMap.containsKey(rs.getInt(2)+"")) {
                    strainNameMap.get(rs.getInt(2)+"").add(name);
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(name);
                    strainNameMap.put(rs.getInt(2)+"", list);
                }

            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    } 
     
      private void loadStrainFamilyKeys() {
     
     
        String sql =  "SELECT  _strain_key, _strainfamily_key from strain";
     
        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql);

            System.out.println("Loading Strain Family Keys");
            rs = ps.executeQuery();
            while (rs.next()) {
                    strainFamilyKeyMap.put(rs.getInt(1)+"",rs.getInt(2)+"");
            }

            rs.close();
            ps.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    } 
     
     
     
    private String deHTML(String in){
        String out =  in.replaceAll("<i>", "").replaceAll("</i>", "").replaceAll("<sub>", "").replaceAll("</sub>", "").replaceAll("<sup>", "").replaceAll("</sup>", "");
        if(out.contains("<") || out.contains(">")){
            System.out.println("THIS MAY HAVE HTML :"+out +" was: "+in);
        }
        return out;
    }

    private void loadParentMaps() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("SELECT o._organ_key, op.name, op._organ_key FROM Organ o, Organ op  where op._organ_key = o._organparent_key");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            System.out.println("Loading Organ Parents");
            rs = ps.executeQuery();
            while (rs.next()) {
                String[] str ={rs.getString(2),rs.getInt(3)+""};
                
                organParentMap.put(rs.getInt(1) + "", str );

            }

            rs.close();
            ps.close();

            query = new StringBuffer("SELECT tc.name, tcp.name FROM TumorClassification tc, TumorClassification tcp  where tcp._tumorclassification_key = tc._TCParent_key");

            ps = conn.prepareStatement(query.toString());

            System.out.println("Loading TC Parents");
            rs = ps.executeQuery();
            while (rs.next()) {
                tumorClassificationParentMap.put(rs.getString(1), rs.getString(2));

            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void loadPubMedIDs() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("Select j.accID,p.accID from Accession j, Accession p where j._object_key = p._object_key");
            query.append(" and j._mtbtypes_key = 6 and p._mtbtypes_key = 6 and j._object_key = p._object_key ");
            query.append(" and j._siteinfo_key = 1 and p._siteinfo_key = 29");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            System.out.println("Loading PubMed IDs");
            rs = ps.executeQuery();
            while (rs.next()) {
                referenceIDMap.put(rs.getString(1), rs.getString(2));

            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    
    // split this into individual fields then combine them in solr if desired
    private void loadReferences() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("Select j.accID, authors, journal, year, title, abstracttext, primaryAuthor from Accession j, Reference r where j._object_key = r._reference_key");
            query.append(" and j._mtbtypes_key = 6 and j._siteinfo_key = 1 ");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            System.out.println("Loading References");
            rs = ps.executeQuery();
            while (rs.next()) {
                String refKey = rs.getString(1);
                if(rs.getString(2)!= null && rs.getString(2).length()>1){
                    String[] authors = rs.getString(2).split(";");
                    ArrayList<String> authorList = new ArrayList();
                    for(String author : authors){
                        authorList.add(author.trim());
                    }
                    authorsMap.put(refKey,authorList);
                }
                journalMap.put(refKey,rs.getString(3)+" "+rs.getInt(4));
                yearMap.put(refKey,rs.getInt(4));
                titleMap.put(refKey,rs.getString(5));
                abstractMap.put(refKey, rs.getString(6));
                
                // what is this used for should be shortCitation from reference table
                citationMap.put(refKey, rs.getString(7)+", "+rs.getString(3)+ " ("+rs.getInt(4)+")");
            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    

    private void loadMinFreqColony() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("Select _tumorfrequency_key from TumorFrequency where freqNum >= ? and colonySize >= ?");

            PreparedStatement ps = conn.prepareStatement(query.toString());
            ps.setInt(1, minFreqNum);
            ps.setInt(2, minColonySize);

            System.out.println("Loading TF matching freq >= " + minFreqNum + " and colonySize >= " + minColonySize);
            rs = ps.executeQuery();
            while (rs.next()) {
                minFreqColony.put(rs.getInt(1)+"", rs.getString(1));

            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void loadSeries() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("select distinct se.id as series, sa._object_key as tf ");
            query.append(" from Series se, Sample s, SampleAssoc sa, SeriesSampleAssoc ssa ");
            query.append(" where se._series_key = ssa._series_key ");
            query.append(" and ssa._sample_key = s._sample_key ");
            query.append(" and sa._sample_key = s._sample_key ");
            query.append(" and  sa._MTBTypes_key = 5");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            rs = ps.executeQuery();
            while (rs.next()) {
                if (seriesMap.containsKey(rs.getString(2))) {
                    seriesMap.get(rs.getString(2)).add(rs.getString(1));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(1));
                    seriesMap.put(rs.getString(1), list);
                }

            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /* mark TFs as based on mutant on 
    
    The hyperlink to the models in the "All Strains" column would send the 
    Anatomical System/Organ specifics plus the 80/20 flag as search constraints to the faceted search.

    For the number in the "Mutant Strains" hyperlink an additional search restriction should be that 
    the strain is attached to an allele pair (except allele pairs that contain only "Normal" alleles, 
    or have "Allele not specified"/"Normal", or have only QTLs don't count [that's the tricky part]).

    For the number in the "Non-mutant Strains" column the inverse should apply, strains with no attached
    allele pair (except where both members of the allele pair are "Normal" or it is "Allele not specified"/"Normal" or it is QTLs).

    
    Mutant = attached to allele pair where no alleles are normal, or have only QTL's
    Non-Mutant are all others.
    

        select s._strain_key
        from Strain s, genotype g, allelepair ap, allele a1
        where s._strain_key = g._strain_key 
        and g._allelepair_key = ap._allelepair_key 
        and ap._allele1_key = a1._allele_key 
        and a1._alleletype_key not in (22,30)
        union
        select s._strain_key
        from Strain s, genotype g, allelepair ap, allele a2
        where s._strain_key = g._strain_key 
        and g._allelepair_key = ap._allelepair_key 
        and ap._allele2_key = a2._allele_key 
        and a2._alleletype_key not in (22,30)
  
     */
    private void loadMutants() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;
            StringBuffer query = new StringBuffer();

            query = new StringBuffer("select s._strain_key");
            query.append(" from Strain s, genotype g, allelepair ap, allele a1");
            query.append(" where s._strain_key = g._strain_key");
            query.append(" and g._allelepair_key = ap._allelepair_key ");
            query.append(" and ap._allele1_key = a1._allele_key ");
            query.append(" and a1._alleletype_key not in (22,30) ");
            query.append(" union ");
            query.append(" select s._strain_key ");
            query.append(" from Strain s, genotype g, allelepair ap, allele a2");
            query.append(" where s._strain_key = g._strain_key");
            query.append(" and g._allelepair_key = ap._allelepair_key ");
            query.append(" and ap._allele2_key = a2._allele_key ");
            query.append(" and a2._alleletype_key not in (22,30)");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            rs = ps.executeQuery();
            while (rs.next()) {
                mutantsMap.put(rs.getInt(1) + "", rs.getInt(1) + "");
            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void loadHumanTissuesMap() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            StringBuffer query = new StringBuffer("Select 'Lung' as human, _organ_key,name from organ");
            query.append(" where _anatomicalsystem_key = 4");
            query.append(" union");
            query.append(" select 'Lymphohematopoietic' as human, _organ_key, name from organ");
            query.append(" where _anatomicalsystem_key = 2  ");
            query.append(" union");
            query.append(" select 'Colon and other intestine' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 17   ");
            query.append(" union");
            query.append(" select 'Pancreas' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 28");
            query.append(" union");
            query.append(" select 'Breast' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 5");
            query.append(" union");
            query.append(" select 'Liver and bile duct' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 26");
            query.append(" union");
            query.append(" select 'Prostate' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 140");
            query.append(" union");
            query.append(" select 'Urinary bladder' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 94");
            query.append(" union");
            query.append(" select 'Brain and other nervous system' as human, _organ_key, name from organ");
            query.append(" where _anatomicalsystem_key = 14");
            query.append(" union");
            query.append(" select 'Esophagus' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 18");
            query.append(" union");
            query.append(" select 'Uterus and cervix' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 66");
            query.append(" union");
            query.append(" select 'Kidney and renal pelvis' as human, _organ_key, name from organ");
            query.append(" where _organparent_key in (36,156)");
            query.append(" union");
            query.append(" select 'Ovary' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 60");
            query.append(" union");
            query.append(" select 'Skin' as human, _organ_key, name from organ");
            query.append(" where _organparent_key in (45,365)");
            query.append(" union");
            query.append(" select 'Stomach' as human, _organ_key, name from organ");
            query.append(" where _organparent_key in (159,15)");
            query.append(" union");
            query.append(" select 'Oral cavity' as human, _organ_key, name from organ");
            query.append(" where _organparent_key in (14,245,31,180,9,390,363)");
            query.append(" union");
            query.append(" select 'Soft tissue including heart' as human, _organ_key, name from organ");
            query.append(" where _anatomicalsystem_key in (3,10)");
            query.append(" union");
            query.append(" select 'Gallbladder' as human, _organ_key, name from organ");
            query.append(" where _organparent_key =193");
            query.append(" union");
            query.append(" select 'Endocrine system' as human, _organ_key, name from organ");
            query.append(" where _anatomicalsystem_key = 12");
            query.append(" union");
            query.append(" select 'Bone and joint' as human, _organ_key, name from organ");
            query.append(" where _organparent_key = 71 or _organ_key in (132,144,196,358,569)");
            query.append(" order by human");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            rs = ps.executeQuery();
            while (rs.next()) {
                humanTissueMap.put(rs.getInt(2) + "", rs.getString(1));
                humanTissueNameMap.put(rs.getString(3), rs.getString(1));
            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void loadExcludeTumorClassificationForModels() {

        try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            StringBuffer query = new StringBuffer("select _tumorclassification_key ");
            query.append("  from tumorclassification ");
            query.append("  where _tcparent_key in (51,84,93,109,146,176,210,251,336,350,907)");

            PreparedStatement ps = conn.prepareStatement(query.toString());

            rs = ps.executeQuery();
            while (rs.next()) {
                excludeTumorClassificationForModels.put(rs.getInt(1) + "", rs.getInt(1) + "");
          //      System.out.println("Will exclude tc key:" + rs.getInt(1) + " for human model");
            }

            rs.close();
            ps.close();

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    // this needs some documentation for sure.
    private void loadSingleMutantModels(){
    StringBuilder sql= new StringBuilder("select _strain_key from (select g._strain_key, count(distinct g._allelepair_key) as cnt from genotype g ");
               sql.append("where _strain_key in (select s._strain_key ");
               sql.append(" from Strain s, genotype g, allelepair ap, allele a1 ");
               sql.append(" where s._strain_key = g._strain_key ");
               sql.append(" and g._allelepair_key = ap._allelepair_key ");
               sql.append(" and ap._allele1_key = a1._allele_key ");
               sql.append(" and a1._alleletype_key not in (22,30))");
               sql.append(" union ");
               sql.append(" select s._strain_key ");
               sql.append(" from Strain s, genotype g, allelepair ap, allele a2 ");
               sql.append(" where s._strain_key = g._strain_key ");
               sql.append(" and g._allelepair_key = ap._allelepair_key ");
               sql.append(" and ap._allele2_key = a2._allele_key ");
               sql.append(" and a2._alleletype_key not in (22,30))");
               sql.append(" group by g._strain_key");
               sql.append(" order by cnt desc) as grp");
               sql.append(" where grp.cnt = 1");
    
    
     try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql.toString());

            System.out.println("Loading Single Marker Models");
            rs = ps.executeQuery();
            
            while (rs.next()) {
               singleMutantModels.put(rs.getInt(1),rs.getString(1));

            }

            rs.close();
            ps.close();
            conn.close();
            System.out.println("Strains identified as single mutants:"+singleMutantModels.size());
            if(true)System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    
    
    private void loadTumorMutationTypes(){
    StringBuilder sql= new StringBuilder("select sub.tfKey, sub.alleleType from ");
                sql.append(" (select tg._tumorfrequency_key as tfKey, at.type as alleleType ");
                sql.append(" from tumorgenetics tg, allelepair ap, allele a, alleletype at ");
                sql.append(" where tg._allelepair_key = ap._allelepair_key and ");
                sql.append(" ap._allele1_key = a._allele_key and a._alleletype_key = at._alleletype_key ");
                sql.append(" union ");
                sql.append(" select tg._tumorfrequency_key as tfKey, at.type as alleleType ");
                sql.append(" from tumorgenetics tg, allelepair ap, allele a, alleletype at ");
                sql.append(" where tg._allelepair_key = ap._allelepair_key and ");
                sql.append(" ap._allele2_key = a._allele_key and a._alleletype_key = at._alleletype_key");
                sql.append(" union ");
                sql.append(" select tgc._tumorfrequency_key as tfKey, at.type as alleleType ");
                sql.append(" from tumorgeneticchanges tgc, alleletype at ");
                sql.append(" where tgc._alleletype_key = at._alleletype_key) as sub");
    
     try {
            DAOManagerMTB manager = DAOManagerMTB.getInstance();
            manager.getConnection();

            Connection conn = manager.getConnection();
            ResultSet rs = null;

            PreparedStatement ps = conn.prepareStatement(sql.toString());

            System.out.println("Loading tumor mutation types");
            rs = ps.executeQuery();
            
            while (rs.next()) {
                while (rs.next()) {
                if (tumorMutationTypeMap.containsKey(rs.getInt(1)+"")) {
                    tumorMutationTypeMap.get(rs.getInt(1)+"").add(rs.getString(2));
                } else {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(rs.getString(2));
                    tumorMutationTypeMap.put(rs.getInt(1)+"", list);
                }
                
                

            }

            }

            rs.close();
            ps.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void connectMTB() throws Exception {

        
        String mtb_jdbc_driver = "org.postgresql.Driver";
        String mtb_jdbc_url = "jdbc:postgresql://bhmtbdb01:5432/mtb";
        String mtb_jdbc_user = "mtb";
            
//        String mtb_jdbc_driver = "org.postgresql.Driver";
//        String mtb_jdbc_url = "jdbc:postgresql://localhost:5432/mtb";
//        String mtb_jdbc_user = "mtb";
        String mtb_jdbc_password = props.getProperty("db_pwd");
        //String mtb_jdbc_password = "";

        DAOManagerMTB manager = DAOManagerMTB.getInstance();

        manager.setJdbcDriver(mtb_jdbc_driver);
        manager.setJdbcPassword(mtb_jdbc_password);
        manager.setJdbcUrl(mtb_jdbc_url);
        manager.setJdbcUsername(mtb_jdbc_user);

       
    }
    
    
    private void addUniqueField(SolrInputDocument doc, String name, String value){
        Collection<Object> values = doc.getFieldValues(name);
        if(values == null || !values.contains(value)){
            doc.addField(name, value);
        }
        
    };
    
    private Double maxFreqsToNum(Collection<String> freaks){
        if(freaks.size() ==0) return null;
        Double max =0d,test = 0d;
        for(String freq : freaks){
            test = freqToDouble(freq);
            if(max < test ){
                max = test;
            }
        }
        return max;
        
        
    }
    
    // sadly copied from MTBStrainTumorSummaryDTO
    public static double freqToDouble(String freq) {
        double ret = -1.0;

        try {
            ret = Double.parseDouble(freq);
        } catch (NumberFormatException nfe) {
            // do nothing
        } catch (Exception e) {
            // do nothing
        }

        if (ret > 0) {
            return ret;
        }

        String comp = freq.toLowerCase();

        if ("very high".equals(comp)) {
            ret = 81.0;
        } else if ("high".equals(comp)) {
            ret = 51.0;
        } else if ("moderate".equals(comp)) {
            ret = 31.0;
        } else if ("low".equals(comp)) {
            ret = 19.0;
         } else if ("very low".equals(comp)) {
            ret = 9.0;
        } else if ("sporadic".equals(comp)) {
            ret = 0.9;
        } else if ("observed".equals(comp)) {
            ret = 0.1;
        } else {
            ret = 0.0;
        }

        return ret;
    }
}
