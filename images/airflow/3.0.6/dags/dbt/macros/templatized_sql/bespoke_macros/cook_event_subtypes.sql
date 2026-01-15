{%- macro cook_event_subtypes() -%}
  , CASE WHEN RLIKE(UPPER(cs.barcode), '133A254\\|(.*)|(.*)') AND
              -- Four chef's recipes are included with ovens as physical barcodes. For some reason, these are formatted like normal Tovala meals.
              ({{ meal_sku_id('UPPER(cs.barcode)') }} IN (SELECT meal_sku_id FROM brine.non_tovala_meal_ids WHERE category = 'qvc')
               OR {{ meal_sku_id('UPPER(cs.barcode)') }} IN (SELECT meal_sku_id FROM {{ ref('meal_skus') }})
               OR NULLIF(TRIM(SPLIT_PART(UPPER(cs.barcode), '|', 2)), '') IN (SELECT short_tag FROM {{ ref('add_ons') }}))
              -- Format should be A|B|C or A|B|C|D
              -- A = meal flag = '133A254
              -- B = meal_sku_id (numeric) OR UUID for coupled add-on OR shortened add-on id for decoupled add-ons
              -- C = 00000001 OR 5E3XXXX OR 5E3XXXXX
              -- D = subroutine (A, B, C, or UNDEFINED)
         THEN 'tovala_meal'  
         WHEN tab.barcode IS NOT NULL 
              OR {{ meal_sku_id('UPPER(cs.barcode)') }} IN (SELECT meal_sku_id FROM brine.non_tovala_meal_ids WHERE category = 'insert') 
         THEN 'tovala_assist_recipe'
         WHEN tpb.barcode IS NOT NULL AND tpb.preset_category = 'a_la_carte'
         THEN 'tovala_preset_recipe'
         WHEN LOWER(tpb.preset_category) IN ('utility', 'quick_start')
              OR cs.barcode ILIKE 'manual-heat%'
              OR cs.barcode ILIKE 'manual-mini-magic%'
         THEN 'preset'
         WHEN cs.barcode IS NULL AND LOWER(cs.type) = 'steadystate'
              AND cs.mode IS NOT NULL
         THEN 'steady_state'        
         WHEN RLIKE(UPPER(cs.barcode), '(MAPP-|ROBO-|TEMP-).*|\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}')
              OR udm.customer_id IS NOT NULL
         THEN 'customer_defined'
         WHEN UPPER(cs.barcode) IN (SELECT DISTINCT UPPER(barcode) FROM {{ table_reference('sts_products') }})
         THEN 'scan_the_store'
    END AS cook_event_subtype
{%- endmacro -%}