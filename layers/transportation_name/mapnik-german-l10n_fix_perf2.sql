CREATE or REPLACE FUNCTION osml10n_geo_translit(name text, place geometry DEFAULT NULL) RETURNS TEXT AS $$
  BEGIN
    -- RAISE LOG 'going to transliterate %', name;
    IF (place IS NULL) THEN
      return osml10n_cc_transscript(name,'aq');
    ELSE
      /*
         Look up the country where the geometry is located
      */
      return(osml10n_cc_transscript(name,NULL));

    END IF;
  END;
$$ LANGUAGE plpgsql STABLE;
