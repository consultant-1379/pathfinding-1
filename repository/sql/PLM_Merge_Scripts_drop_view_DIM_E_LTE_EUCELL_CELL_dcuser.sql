IF (select count(*) from sys.sysviews where viewname like 'DIM_E_LTE_EUCELL_CELL' and vcreator='dc') > 0 THEN drop view dc.DIM_E_LTE_EUCELL_CELL; END IF;
