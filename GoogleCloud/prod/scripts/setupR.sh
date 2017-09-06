#!/bin/bash

#exec >>/root/$(basename $0).log.$(date +%d) 2>&1
now="$(date +'%Y-%m-%d-%H-%M-%S')"

exec >>/root/setupR_${now}.log 2>&1

set -x

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

	
#       cd /tmp
       sed -i -e 's/org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator/org.apache.hadoop.yarn.util.resource.DominantResourceCalculator/g' /usr/lib/hadoop-yarn/etc/hadoop/capacity-scheduler.xml
        
	sudo chmod 777 /root
        sudo mkdir /root/output
        sudo mkdir /root/scripts
        sudo mkdir /root/forecast
	sudo mkdir /root/lib
	sudo chmod 777 /root/output
        sudo chmod 777 /root/scripts
        sudo chmod 777 /root/forecast
	sudo chmod 777 /root/lib
	#sudo gsutil cp gs://shc-afe-dev.appspot.com/prod/scripts/install*Package.r /root/scripts/
	sudo gsutil cp gs://shc-afe-dev.appspot.com/prod/rpackages/*.tar /usr/local/lib/R/site-library/
	sudo gsutil cp gs://shc-afe-dev.appspot.com/prod/scripts/lib/* /root/lib/
	sudo chmod -R 777 /root/lib/*	
        
	sudo apt-get install -y expect
        
	#cd /root
	cd /usr/local/lib/R/site-library
	for i in *; 
	do
		echo "untaring i$" 
		tar xvf $i;
		if [ $? -ne "0" ]; then
			echo "untarring $i failed"
			exit 1
		else
		   	echo "untarring $i succsss"
		fi
		   
	done

	#echo "Installing installForecastPackage.r `date`"
	#sudo R CMD BATCH /root/scripts/installForecastPackage.r /root/installForecastPackage_${now}.log
	#grep "ERROR" /root/installForecastPackage_${now}.log  > /root/installForecastPackage_${now}.err
	#SIZE=`wc -c < /root/installForecastPackage_${now}.err`
	#if [ "$SIZE" -ge "1" ]; then
	#	echo "*** Install of installForecastPackage.r failed `date`.  Check Log /root/installForecastPackage_${now}.log"
        #        exit 1
	#else
	#	echo "Install of installForecastPackage.r completed successfully.  `date`"
	#fi

	#echo "Installing installPlyrPackage.r `date`"
        #sudo R CMD BATCH /root/scripts/installPlyrPackage.r /root/installPlyrPackage_${now}.log
	#grep "ERROR" /root/installPlyrPackage_${now}.log  > /root/installPlyrPackage_${now}.err
        #SIZE=`wc -c < /root/installPlyrPackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installPlyrPackage.r failed `date`.  Check Log /root/installPlyrPackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installPlyrPackage.r completed successfully.  `date`"
        #fi

	#echo "Installing installStringdistPackage.r `date`"
        #sudo R CMD BATCH /root/scripts/installStringdistPackage.r /root/installStringdistPackage_${now}.log
	#grep "ERROR" /root/installStringdistPackage_${now}.log  > /root/installStringdistPackage_${now}.err
        #SIZE=`wc -c < /root/installStringdistPackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installStringdistPackage.r failed `date`.  Check Log /root/installStringdistPackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installStringdistPackage.r completed successfully.  `date`"
        #fi


	#echo "Installing installSqldfPackage.r `date`"
        #sudo R CMD BATCH /root/scripts/installSqldfPackage.r /root/installSqldfPackage_${now}.log
	#grep "ERROR" /root/installSqldfPackage_${now}.log  > /root/installSqldfPackage_${now}.err
        #SIZE=`wc -c < /root/installSqldfPackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installSqldfPackage.r failed `date`.  Check Log /root/installSqldfPackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installSqldfPackage.r completed successfully.  `date`"
        #fi

	#echo "Installing installDatatablePackage.r `date`"
        #sudo R CMD BATCH /root/scripts/installDatatablePackage.r /root/installDatatablePackage_${now}.log
	#grep "ERROR" /root/installDatatablePackage_${now}.log  > /root/installDatatablePackage_${now}.err
        #SIZE=`wc -c < /root/installDatatablePackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installDatatablePackage failed `date`.  Check Log /root/installDatatablePackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installDatatablePackage completed successfully.  `date`"
        #fi

	#echo "Installing installDplyrPackage.r `date`"
        #sudo R CMD BATCH /root/scripts/installDplyrPackage.r /root/installDplyrPackage_${now}.log
        #grep "ERROR" /root/installDplyrPackage_${now}.log  > /root/installDplyrPackage_${now}.err
        #SIZE=`wc -c < /root/installDplyrPackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installDplyrPackage.r failed `date`.  Check Log /root/installDplyrPackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installDplyrPackage.r completed successfully.  `date`"
        #fi

