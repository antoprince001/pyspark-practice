# Find matching hosts and guests pairs in a way that they are both of the same gender and nationality.
# Output the host id and the guest id of matched pair.



import pyspark

airbnb_hosts = airbnb_hosts.join(airbnb_guests, ['gender', 'nationality'], 'inner').select('guest_id','host_id').dropDuplicates()

airbnb_hosts.toPandas()
