#!/bin/bashexit

#All the below scripts will work based on the data provided by acadgild as data/web/file.xml and data/mob/file.txt

sh /home/acadgild/project/scripts/start-daemoon.sh

sh /home/acadgild/project/scripts/populate-lookup.sh

sh /home/acadgild/project/scripts/data_enrichment_filtering_schema.sh

sh /home/acadgild/project/scripts/dataformatting.sh

sh /home/acadgild/project/scripts/data_enrichment.sh

sh /home/acadgild/project/scripts/data_analysis.sh
