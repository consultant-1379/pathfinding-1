/**------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.*;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for creating the loader set for a ENIQ EVENTS techpack.
 * 
 * @author epaujor
 * 
 */
public class EventsCreateLoaderSet extends CreateLoaderSet {

    private static final String INTERVAL = "interval";

    public EventsCreateLoaderSet(final String templateDir, final String name, final String version, final String versionid,
                                 final RockFactory dwhrepRock, final RockFactory rock, final int techPackID, final String techPackName,
                                 final boolean schedulings) throws SQLException {
        super(templateDir, name, version, versionid, dwhrepRock, rock, techPackID, techPackName, schedulings);
    }

    @Override
    protected boolean isSqlTemplateGoingToBeEmpty() {
        return com.ericsson.eniq.common.Utils.isEventsTechpack(this.techPackName);
    }

    @Override
    protected String createWhereClauseForMetaTransferActions(final String measTypeName, final String tailDir) throws IOException {
        final Properties prop = createPropertiesForMetaTransferActions(measTypeName, tailDir);
        final String versiondir = Utils.getEventsProperty("versiondir", "1");
        prop.setProperty("versiondir", versiondir);

        return Utils.propertyToString(prop);
    }

    @Override
    protected void createUpdateDimSessionAction(final long iSet, final long iAction, final String element, final Measurementtype measType,
                                                final Measurementtable measTable, final int order) {
        // This does nothing for EVENTS. UpdateDimSession does not need to be
        // created for EVENTS.
    }

    @Override
    protected void createDuplicateCheckAction(final long iSet, final long iAction, final Measurementtype measType, final Measurementtable measTable,
                                              final int order) {
        // This does nothing for EVENTS. DuplicateCheck does not need to be
        // created for EVENTS.
    }

    @Override
    protected String getLoaderActionTypeName() {
        return Constants.EVENT_LOADER;
    }

    @Override
    protected boolean isValidSet(final Measurementtable measTable) {
        return (measTable.getTablelevel().equalsIgnoreCase("raw") || measTable.getTablelevel().equalsIgnoreCase("plain") || (measTable
                .getTablelevel().equalsIgnoreCase("15min") && isDataTieringTechPack() && isDataTieringMeasType(measTable)));
    }

    @Override
    protected Properties createPropertiesForMetaTransferActions(final String measTypeName, final String tailDir) {
        final Properties prop = new Properties();

        prop.setProperty("tablename", measTypeName);
        prop.setProperty("techpack", this.techPackName);
        prop.setProperty("taildir", tailDir);
        if (tailDir.equalsIgnoreCase("15min") && isDataTieringTechPack()) {
            prop.setProperty("dateformat", "yyyyMMdd");
        } else {
            prop.setProperty("dateformat", "yyyy-MM-dd");
        }
        return prop;
    }

    @Override
    public void updateLoaderAction(final String mType) throws Exception {
        /*
         * 1.) Search for the meas type in the MeasurementType table to get the joinable field. 2.) Check if the joinable is null or empty - implies
         * Loader else UnPartitioned_Loader. 3.) Get the setId from Utils.getSetId 4.) Create the loader sql query. 5.) Update the
         * meta_transfer_actions record with the new loader query.
         */

        final Measurementtype measTypeWhere = new Measurementtype(dwhrepRock);
        measTypeWhere.setTypeid(mType);
        final MeasurementtypeFactory measTypeFactory = new MeasurementtypeFactory(dwhrepRock, measTypeWhere);
        final Vector<Measurementtype> measTypeList = measTypeFactory.get();
        final Measurementtype measTypeValue = measTypeList.get(0);

        final String joinable = measTypeValue.getJoinable();
        String loaderType = null;
        if (joinable != null && joinable.length() > 0) {
            loaderType = "UnPartitioned_Loader";
        } else {
            loaderType = "Loader";
        }

        log.info("Joinable:" + joinable + " loaderType:" + loaderType);
        final long setid = Utils.getSetId("Loader" + "_" + getTypeName(measTypeValue), version, techPackID, rock);

        if (setid == -1) {
            log.info("We have a problem");
            throw new Exception("Unable to find the set in the meta_collections_set for meas type:" + getTypeName(measTypeValue) + " version:"
                    + version + " techPackID:" + techPackID);
        }
        final long tpId = techPackID;

        final Meta_transfer_actions mta = new Meta_transfer_actions(rock);
        mta.setTransfer_action_name(loaderType + "_" + getTypeName(measTypeValue));
        mta.setVersion_number(version);
        mta.setCollection_set_id(tpId);
        mta.setCollection_id(setid);
        final Meta_transfer_actionsFactory mtaF = new Meta_transfer_actionsFactory(rock, mta);
        if (mtaF.size() > 0) {
            final Meta_transfer_actions transferActionsObject = mtaF.getElementAt(0);
            log.info("Existing action_content:" + transferActionsObject.getAction_contents());
            String mTableId = null;
            if (isDataTieringTechPack()) {
                mTableId = mType + ":15MIN";
            } else {
                mTableId = mType + ":RAW";
            }
            final String sqlQuery = CreateSql(mTableId);
            log.info("New action_content:" + sqlQuery);
            transferActionsObject.setAction_contents(sqlQuery);
            final int updateValue = transferActionsObject.updateDB();
            log.info("Update Value:" + updateValue);
            if (updateValue == 1) {
                log.info("Updated the loader sql with updateValue:" + updateValue + " for set:" + setid);
            }
        } else {
            log.info("Unable to find the meta_tranfer_actions record for tpId:" + techPackID + " setId:" + setid + " version:" + version);
            throw new Exception("Unable to update the loader sql. Not able to find record in meta_transfer_actions");
        }
    }

    /**
     * EVENT_E_LTE & EVENT_E_SGEH are consider Data Tiering tech packs.
     * 
     * @return
     */
    protected boolean isDataTieringTechPack() {
        boolean result = false;
        String dataTieringTps = StaticProperties.getProperty("dataTieringTps", "EVENT_E_LTE,EVENT_E_SGEH,EVENT_E_RAN_CFA");

        for (String dataTieringTp : dataTieringTps.split(",")) {
            if ((this.techPackName != null) && (this.techPackName.endsWith(dataTieringTp))) {
                result = true;
            }
        }
        return result;
    }

    /**
     * @param measTable
     * @return
     */
    protected boolean isDataTieringMeasType(final Measurementtable measTable) {
        boolean result = false;
        String dataTieringMeasTypes = StaticProperties.getProperty("dataTieringMeasType", "_SUC,_ALLCALLS");

        for (String dataTieringMeasType : dataTieringMeasTypes.split(",")) {
            if ((measTable.getTypeid() != null) && (measTable.getTypeid().endsWith(dataTieringMeasType))) {
                result = true;
            }
        }
        return result;
    }

    @Override
    protected ActionGenerator createActionType(final Measurementtype measType, final Measurementtable measTable, final String typeName,
                                               final int order, final String createSql) throws Exception {
        final String tableLevel = measTable.getTablelevel();
        final String loaderActionType;

        if (tableLevel.equalsIgnoreCase("15min") && isDataTieringTechPack() && isDataTieringMeasType(measTable)) {
            loaderActionType = Constants.TIMEBASE_PARTITION_LOADER;
        } else {
            loaderActionType = Constants.EVENT_LOADER;
        }

        final ActionGenerator loaderActionGenerator = createAction("Loader_" + typeName, order, loaderActionType,
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType, measTable,
                createWhereClauseForMetaTransferActions(getTypeName(measType), measTable.getTablelevel().toLowerCase()), createSql);

        return loaderActionGenerator;
    }

    @Override
    protected long createScheduling(final String holdFlag, final String schedulingName) throws SQLException, RockException, Exception {
        createIntervalSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID, new Long(this.maxSetID + iSet).intValue(),
                schedulingName, holdFlag).insertToDB(this.rock);
        iScheduling++;
        return iScheduling;
    }

    @Override
    protected int createExtraActions(final int order, final String typeName, final Measurementtable measTable) throws Exception {
        int ord = order;
        ord = createCountingIntervalAction(ord);

        ord = createCountingTriggerAction(typeName, ord, measTable);

        ord = createStoreCountDataAction(ord);
        return ord;
    }

    /**
     * Create "StoreCountingData" action in loader set
     * 
     * @param order
     * @param order
     * @return
     * @throws Exception
     * @throws SQLException
     * @throws RockException
     */
    private int createStoreCountDataAction(int order) throws Exception, SQLException, RockException {
        final String storeCountingDataActionName = "Store_ROWIDs";
        log.info("  Creating action " + storeCountingDataActionName);
        final ActionGenerator storeCountingDataAction = createAction(storeCountingDataActionName, order, "StoreCountingData", (this.maxSetID + iSet),
                (maxActionID + iAction), "", "");
        // CONNECTION_ID = 2 is dwh
        storeCountingDataAction.setCONNECTION_ID("2");
        storeCountingDataAction.insertToDB(rock);
        iAction++;
        order++;
        return order;
    }

    /**
     * Create trigger action for count in loader set
     * 
     * @param order
     * @param typeName
     * @param order
     * @return
     * @throws Exception
     * @throws IOException
     * @throws SQLException
     * @throws RockException
     */
    private int createCountingTriggerAction(final String typeName, int order, final Measurementtable measTable) throws Exception, IOException,
            SQLException, RockException {
        String lockTable = typeName;
        String type = "CountingTrigger";

        if (measTable.getTablelevel().equalsIgnoreCase("15min") && isDataTieringTechPack() && isDataTieringMeasType(measTable)) {
            lockTable = typeName + "_DAY";
            type = "CountingDayTrigger";
        }

        final String countSetName = "Count_" + lockTable;
        final String countTriggerActionName = "Trigger_" + countSetName;

        final Properties prop = new Properties();

        prop.setProperty("lockTable", lockTable);
        prop.setProperty("setName", countSetName);
        prop.setProperty("timelevels", "1440");

        log.info("  Creating action " + countTriggerActionName);

        final ActionGenerator countTriggerAction = createAction(countTriggerActionName, order, type, (this.maxSetID + iSet), (maxActionID + iAction),
                Utils.propertyToString(prop), "");
        countTriggerAction.insertToDB(rock);
        iAction++;
        order++;
        return order;
    }

    /**
     * Create "CountingIntervals" action in loader set
     * 
     * @param order
     * @param order
     * @return
     * @throws Exception
     * @throws SQLException
     * @throws RockException
     */
    private int createCountingIntervalAction(int order) throws Exception, SQLException, RockException {
        final String countIntervalsActionName = "Timestamp_Action";
        log.info("  Creating action " + countIntervalsActionName);
        final ActionGenerator countIntervalsAction = createAction(countIntervalsActionName, order, "CountingIntervals", (this.maxSetID + iSet),
                (maxActionID + iAction), "", "");
        countIntervalsAction.insertToDB(rock);
        iAction++;
        order++;
        return order;
    }

    private ScheduleGenerator createIntervalSchedule(final long scheduleID, final long techpackID, final long setID, final String schedulingName,
                                                     final String holdFlag) throws Exception {

        final ScheduleGenerator schedule = new ScheduleGenerator();
        schedule.setVERSION_NUMBER(this.version);
        schedule.setID(new Long(scheduleID));
        schedule.setEXECUTION_TYPE(INTERVAL);
        schedule.setCOLLECTION_SET_ID(new Long(techpackID));
        schedule.setCOLLECTION_ID(new Long(setID));
        schedule.setNAME(schedulingName);
        schedule.setHOLD_FLAG(holdFlag);
        // Default to running once every minute
        schedule.setSCHEDULING_MONTH(new Long(1));
        schedule.setSCHEDULING_DAY(new Long(1));
        schedule.setSCHEDULING_HOUR(new Long(0));
        schedule.setSCHEDULING_MIN(new Long(0));
        schedule.setINTERVAL_HOUR(new Long(0));
        schedule.setINTERVAL_MIN(new Long(1));
        schedule.setSCHEDULING_YEAR(new Long(2012));

        return schedule;
    }
}
