-- Remove all components related to the Module when uninstalling.
begin
    util_module.delete_module_packages(:p_module_id);
end;