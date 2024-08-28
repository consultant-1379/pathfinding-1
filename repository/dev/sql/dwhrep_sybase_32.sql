/*==============================================================*/
/* Table: ReferenceTable and ReferenceColumn                    */
/* BASEDEF removed from the primary key.                        */
/*==============================================================*/
alter table dwhrep.ReferenceColumn drop primary key;
alter table dwhrep.ReferenceTable drop primary key;

alter table dwhrep.ReferenceColumn
   add primary key (TYPEID, DATANAME);
   
alter table dwhrep.ReferenceTable
     add primary key (TYPEID);
