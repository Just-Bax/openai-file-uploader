--It is important to activate rules and automations after rebuilding Objrefs and importing PL/SQL packages
begin
    util_module.enable_module_rules(:p_module_id);
end;