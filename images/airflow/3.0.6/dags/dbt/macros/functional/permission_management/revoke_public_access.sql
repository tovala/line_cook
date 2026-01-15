-- Revokes access to DATABASE and SCHEMAS from the role PBULIC
-- Necessary b/c Stitch automatically grants PUBLIC permissions on any schemas created by Stitch 
{%- macro revoke_public_access() -%}
    {{ alter_access_on('ALL PRIVILEGES', ['DATABASE', 'ALL SCHEMAS IN DATABASE'], ['MASALA'], 'ROLE', 'PUBLIC', optional_qualifier = 'CASCADE', grant_or_revoke = 'REVOKE') }}
{%- endmacro -%}