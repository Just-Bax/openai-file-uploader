--SQL to be executed during Module installation must be placed in the numbered *.sql files.
--Each file can include a single PL/SQL block
--PL/SQL block termination characters are not supported
--PL/SQL will be executed in the _pkg DB schema
--In case of an error, module installation cannot proceed
--DBMS Output will be shown in UI
--All changes will be committed automatically on script completion

begin
    pkg_objref.validate_rebuild_id_pkg_and_types();
    pkg_objref.rebuild_id_pkg_and_types();
    dbms_output.put_line('Object references were rebuilt');
end;
