#!/bin/bash

#exec >>/root/$(basename $0).log.$(date +%d) 2>&1
now="$(date +'%Y-%m-%d-%H-%M-%S')"

exec >>/root/setupR_${now}.log 2>&1

set -x

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

	
       cd /tmp
       sed -i -e 's/org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator/org.apache.hadoop.yarn.util.resource.DominantResourceCalculator/g' /usr/lib/hadoop-yarn/etc/hadoop/capacity-scheduler.xml
        
        mkdir output
        mkdir scripts
        mkdir forecast
	mkdir lib
	chmod 777 /tmp/output
        chmod 777 /tmp/scripts
        chmod 777 /tmp/forecast
	chmod 777 /tmp/lib
	gsutil cp gs://shc-afe-dev.appspot.com/prod/scripts/install*Package.r /tmp/scripts/
	gsutil cp gs://shc-afe-dev.appspot.com/prod/scripts/lib/* /tmp/lib/
        
	apt-get install -y expect
        
	#sudo apt-get -y install r-cran-rcpp
        #wget https://cran.r-project.org/src/contrib/Archive/RcppArmadillo/RcppArmadillo_0.5.000.0.tar.gz
        #tar -zxf RcppArmadillo_0.5.000.0.tar.gz
        #sudo R CMD INSTALL RcppArmadillo
        #wget https://github.com/gagolews/stringi/archive/master.zip
        #unzip master.zip
        #sudo R CMD INSTALL stringi-master
      
	sudo apt-get -y install libcurl4-gnutls-dev
        sudo apt-get -y install libssl-dev

	cd /root

	echo "Installing installForecastPackage.r `date`"
	sudo R CMD BATCH /tmp/scripts/installForecastPackage.r /root/installForecastPackage_${now}.log
	grep "ERROR" /root/installForecastPackage_${now}.log  > /root/installForecastPackage_${now}.err
	SIZE=`wc -c < /root/installForecastPackage_${now}.err`
	if [ "$SIZE" -ge "1" ]; then
		echo "*** Install of installForecastPackage.r failed `date`.  Check Log /root/installForecastPackage_${now}.log"
                exit 1
	else
		echo "Install of installForecastPackage.r completed successfully.  `date`"
	fi

	echo "Installing installPlyrPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installPlyrPackage.r /root/installPlyrPackage_${now}.log
	grep "ERROR" /root/installPlyrPackage_${now}.log  > /root/installPlyrPackage_${now}.err
        SIZE=`wc -c < /root/installPlyrPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installPlyrPackage.r failed `date`.  Check Log /root/installPlyrPackage_${now}.log"
                exit 1
        else
                echo "Install of installPlyrPackage.r completed successfully.  `date`"
        fi

	echo "Installing installStringdistPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installStringdistPackage.r /root/installStringdistPackage_${now}.log
	grep "ERROR" /root/installStringdistPackage_${now}.log  > /root/installStringdistPackage_${now}.err
        SIZE=`wc -c < /root/installStringdistPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installStringdistPackage.r failed `date`.  Check Log /root/installStringdistPackage_${now}.log"
                exit 1
        else
                echo "Install of installStringdistPackage.r completed successfully.  `date`"
        fi


	echo "Installing installSqldfPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installSqldfPackage.r /root/installSqldfPackage_${now}.log
	grep "ERROR" /root/installSqldfPackage_${now}.log  > /root/installSqldfPackage_${now}.err
        SIZE=`wc -c < /root/installSqldfPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installSqldfPackage.r failed `date`.  Check Log /root/installSqldfPackage_${now}.log"
                exit 1
        else
                echo "Install of installSqldfPackage.r completed successfully.  `date`"
        fi

	echo "Installing installDatatablePackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installDatatablePackage.r /root/installDatatablePackage_${now}.log
	grep "ERROR" /root/installDatatablePackage_${now}.log  > /root/installDatatablePackage_${now}.err
        SIZE=`wc -c < /root/installDatatablePackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installDatatablePackage failed `date`.  Check Log /root/installDatatablePackage_${now}.log"
                exit 1
        else
                echo "Install of installDatatablePackage completed successfully.  `date`"
        fi

	echo "Installing installHttrPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installHttrPackage.r /root/installHttrPackage_${now}.log
        grep "ERROR" /root/installHttrPackage_${now}.log  > /root/installHttrPackage_${now}.err
        SIZE=`wc -c < /root/installHttrPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installHttrPackage.r failed `date`.  Check Log /root/installHttrPackage_${now}.log"
                exit 1
        else
                echo "Install of installHttrPackage.r completed successfully.  `date`"
        fi

	echo "Installing installJsonlitePackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installJsonlitePackage.r /root/installJsonlitePackage_${now}.log
        grep "ERROR" /root/installJsonlitePackage_${now}.log  > /root/installJsonlitePackage_${now}.err
        SIZE=`wc -c < /root/installJsonlitePackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installJsonlitePackage.r failed `date`.  Check Log /root/installJsonlitePackage_${now}.log"
                exit 1
        else
                echo "Install of installJsonlitePackage.r completed successfully.  `date`"
        fi


	echo "Installing installR6Package.r `date`"
        sudo R CMD BATCH /tmp/scripts/installR6Package.r /root/installR6Package_${now}.log
        grep "ERROR" /root/installR6Package_${now}.log  > /root/installR6Package_${now}.err
        SIZE=`wc -c < /root/installR6Package_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installR6Package.r failed `date`.  Check Log /root/installR6Package_${now}.log"
                exit 1
        else
                echo "Install of installR6Package.r completed successfully.  `date`"
        fi

	echo "Installing installDplyrPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installDplyrPackage.r /root/installDplyrPackage_${now}.log
        grep "ERROR" /root/installDplyrPackage_${now}.log  > /root/installDplyrPackage_${now}.err
        SIZE=`wc -c < /root/installDplyrPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installDplyrPackage.r failed `date`.  Check Log /root/installDplyrPackage_${now}.log"
                exit 1
        else
                echo "Install of installDplyrPackage.r completed successfully.  `date`"
        fi

	echo "Installing installHttpuvPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installHttpuvPackage.r /root/installHttpuvPackage_${now}.log
        grep "ERROR" /root/installHttpuvPackage_${now}.log  > /root/installHttpuvPackage_${now}.err
        SIZE=`wc -c < /root/installHttpuvPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installHttpuvPackage.r failed `date`.  Check Log /root/installHttpuvPackage_${now}.log"
                exit 1
        else
                echo "Install of installHttpuvPackage.r completed successfully.  `date`"
        fi
	
	echo "Installing installBigqueryPackage.r `date`"
        sudo R CMD BATCH /tmp/scripts/installBigqueryPackage.r /root/installBigqueryPackage_${now}.log
	grep "ERROR" /root/installBigqueryPackage_${now}.log  > /root/installBigqueryPackage_${now}.err
        SIZE=`wc -c < /root/installBigqueryPackage_${now}.err`
        if [ "$SIZE" -ge "1" ]; then
                echo "*** Install of installBigqueryPackage failed `date`.  Check Log /root/installBigqueryPackage_${now}.log"
                exit 1
        else
                echo "Install of installBigqueryPackage completed successfully.  `date`"
        fi

	#echo "Installing installGoogleAuthRPackage.r `date`"
        #sudo R CMD BATCH /tmp/scripts/installGoogleAuthRPackage.r /root/installGoogleAuthRPackage_${now}.log
	#grep "ERROR" /root/installGoogleAuthRPackage_${now}.log  > /root/installGoogleAuthRPackage_${now}.err
        #SIZE=`wc -c < /root/installGoogleAuthRPackage_${now}.err`
        #if [ "$SIZE" -ge "1" ]; then
        #        echo "*** Install of installGoogleAuthRPackage failed `date`.  Check Log /root/installGoogleAuthRPackage_${now}.log"
        #        exit 1
        #else
        #        echo "Install of installGoogleAuthRPackage completed successfully.  `date`"
        #fi

sudo R CMD BATCH /tmp/scripts/installLubridatePackage.r /root/installLubridatePackage_${now}.log
grep "ERROR" /root/installLubridatePackage_${now}.log  > /root/installLubridatePackage_${now}.err
SIZE=`wc -c < /root/installLubridatePackage_${now}.err`
if [ "$SIZE" -ge "1" ]; then
echo "*** Install of installLubridatePackage.r failed `date`.  Check Log /root/installLubridatePackage_${now}.log"
exit 1
else
echo "Install of installLubridatePackage.r completed successfully.  `date`"
fi

