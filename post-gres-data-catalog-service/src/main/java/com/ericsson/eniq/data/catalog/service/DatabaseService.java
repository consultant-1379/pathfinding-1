package com.ericsson.eniq.data.catalog.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class DatabaseService {

    Logger logger = LoggerFactory.getLogger(DatabaseService.class);

    @Autowired
    @Qualifier("dwhDBTemplate")
    JdbcTemplate dwhDBTemplate;

    @Autowired
    @Qualifier("etlrepDBTemplate")
    JdbcTemplate etlrepDBTemplate;


    public List<Map<String,Object>> loadDataFormats(String techPackNames){

        String query_base = "select im.tagid, im.dataformatid, df.foldername, im.transformerid"
                + " from datainterface di, interfacemeasurement im, dataformat df"
                + " where di.interfacename = im.interfacename and im.dataformatid = df.dataformatid"
                + " and di.status = 1 and im.status = 1 and df.versionid in (select versionid from "
                + "dwhrep.tpactivation where status = 'ACTIVE' and techpack_name in (%s)) "
                + "and im.dataformatid like '%%:mdc' ORDER BY im.dataformatid";

        String[] techpacks = techPackNames.split(",");
        String inSQl = String.join(",",Collections.nCopies(techpacks.length,"?"));
        final String query = String.format(query_base, inSQl);

        return dwhDBTemplate.queryForList(query,techpacks);
    }



    public List<Map<String,Object>> loadDataItems(String techPackNames){
        String query_base = "SELECT di.dataname, di.colnumber, di.dataid, di.process_instruction, di.dataformatid, di.datatype, di.datasize, di.datascale,"
                + " COALESCE("
                + " (SELECT 1 FROM MeasurementCounter mc WHERE di.dataname = mc.dataname AND df.typeid = mc.typeid),"
                + " (SELECT 1 FROM ReferenceColumn rc WHERE di.dataname = rc.dataname AND df.typeid = rc.typeid AND uniquekey = 0),"
                + " 0) AS is_counter FROM dwhrep.dataformat df JOIN "
                + "dwhrep.dataitem di ON df.dataformatid = di.dataformatid WHERE df.versionid in (select versionid from "
                + "dwhrep.tpactivation where status = 'ACTIVE' and techpack_name in (%s)) "
                + "and di.dataformatid like '%%:mdc'";

        String[] techpacks = techPackNames.split(",");
        String inSQl = String.join(",",Collections.nCopies(techpacks.length,"?"));
        final String query = String.format(query_base, inSQl);

        return dwhDBTemplate.queryForList(query,techpacks);
    }


    public List<Map<String,Object>> getTransformations(String techPackNames){

        final String query_base = "select tf.transformerid, tf.type, tf.source, tf.target, tf.config from Transformation tf where tf.transformerid IN " +
                "(select t.transformerid from Transformer t where t.transformerid like '%%:mdc' and t.versionid IN" +
                "(select tpa.versionid from Tpactivation tpa where tpa.status = 'ACTIVE' and tpa.techpack_name in (%s)))";

        String[] techpacks = techPackNames.split(",");
        String inSQl = String.join(",",Collections.nCopies(techpacks.length,"?"));
        final String query = String.format(query_base, inSQl);

        return dwhDBTemplate.queryForList(query,techpacks);


    }


    public List<Map<String,Object>> getTransferActions(String collectionSetName){
        final String query_base = "select ta.action_type, ta.order_by_no,ta.connection_id,ta.where_clause_01," +
                "ta.action_contents_01,ta.where_clause_02,ta.action_contents_02,ta.where_clause_03," +
                "ta.action_contents_03 from meta_transfer_actions ta where ta.collection_id IN(" +
                "        select collection_id from meta_collections " +
                "        where settype = 'Loader' and enabled_flag = 'Y' " +
                "                and collection_set_id IN ( select collection_set_id from meta_collection_sets " +
                "where collection_set_name = ? and enabled_flag = 'Y'))";


        return etlrepDBTemplate.queryForList(query_base,collectionSetName);
    }


    public List<Map<String,Object>> getAggregations(String collectionSetName){
        final String query_base = "select ta.action_type, ta.order_by_no,ta.connection_id,ta.where_clause_01," +
                "ta.action_contents_01,ta.where_clause_02,ta.action_contents_02,ta.where_clause_03," +
                "ta.action_contents_03 from meta_transfer_actions ta where ta.collection_id IN(" +
                "        select collection_id from meta_collections " +
                "        where settype = 'Aggregator' and enabled_flag = 'Y' " +
                "                and collection_set_id IN ( select collection_set_id from meta_collection_sets " +
                "where collection_set_name = ? and enabled_flag = 'Y'))";


        return etlrepDBTemplate.queryForList(query_base,collectionSetName);
    }


    public List<Map<String,Object>> getActiveVersionings(){
        final String query = "select techpack_name,versionid from tpactivation " +
                "where status='ACTIVE' ORDER BY versionid";

        return dwhDBTemplate.queryForList(query);
    }


    public List<Map<String,Object>> getActivePMTypeVersionings(){
        final String query = "select techpack_name,versionid from tpactivation " +
                "where status='ACTIVE' and type='PM' ORDER BY versionid";

        return dwhDBTemplate.queryForList(query);
    }


    public List<Map<String,Object>> getBusyHourSrcTable(String typeId){
        final String query_base = "select basetablename from measurementtable" +
                " where (tablelevel = 'RAW' OR tablelevel ='COUNT') " +
                " and typeid = ?";
        return dwhDBTemplate.queryForList(query_base,typeId);

    }


    public List<Map<String,Object>> getBusyHourRefTable(String versionId){
        final String query_base = "select objectname from referencetable where" +
                " table_type='VIEW' and versionid= ?" ; //'DC_E_GGSN:((227))'

        return dwhDBTemplate.queryForList(query_base,versionId);
    }


    public List<Map<String,Object>> getBusyHours(String versionId){
        final String query_base = "select bhlevel,bhtype,bhcriteria,whereclause,description,bhobject,bhelement from" +
                " busyhour where versionid= ? order by targetversionid" ;
        return dwhDBTemplate.queryForList(query_base,versionId);
    }


    public List<Map<String,Object>> getBusyHourPlaceHolders(String versionId){
        final String query_base = "select bhlevel, productplaceholders,customplaceholders from busyhourplaceholders" +
                " where versionid= ?" ;//
        return dwhDBTemplate.queryForList(query_base,versionId);

    }


    public List<Map<String,Object>> getDeltaViews(String versionId){
        final String query_base = "select typeid,vendorrelease from measurementdeltacalcsupport where versionid = ?";

        return dwhDBTemplate.queryForList(query_base,versionId);

    }





}
