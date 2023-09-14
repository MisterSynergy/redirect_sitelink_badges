# Redirect sitelink badges
This is a Wikidata bot that manages badges on sitelinks to redirect pages.

It adds or removes sitelink badges as described in [Wikidata:Sitelinks to redirects](https://www.wikidata.org/wiki/Wikidata:Sitelinks_to_redirects). In particular, it does:
* Add "sitelink to redirect" badge to sitelinks to redirects without any sitelink badge
* Remove "sitelink to redirect" badge and/or "intentional sitelink to redirect" badge from sitelinks to regular (i.e. non-redirect) pages
* Remove "sitelink to redirect" badge from sitelinks that also carry the "intentional sitelink to redirect" badge
* Remove sitelink entirely if the redirect page points to a non-existent target (and is smaller than 100 Bytes; else add "sitelink to redirect" badge since this seems to be an error in the client wiki then)
* Write a report about sitelinks to redirects with redirect targets that are not connected to any Wikidata item; in these situations, the redirect sitelink might either be updated in order to point to the redirect target directly, or the redirect target should be connected to another Wikidata item. This needs to be checked manually, thus a report is written for these situations.

## Technical requirements
The bot is currently scheduled to run weekly on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) from within the `msynbot` tool account. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.11.2.
