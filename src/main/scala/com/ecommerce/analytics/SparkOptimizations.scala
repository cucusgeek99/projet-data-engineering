package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

object SparkOptimizations {

    def cacheIfEnabled(df: DataFrame): DataFrame = {
        if (ConfigLoader.getBoolean("app.optimization.enable-cache", true)) df.cache()
        else df
    }

    // Pour les datasets trpop gros
    def persistLarge(df: DataFrame): DataFrame = {
        if (ConfigLoader.getBoolean("app.optimization.enable-cache", true))
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        else df
    }


    def release(df: DataFrame): Unit = df.unpersist()

    // Pour forcer un broadcast join pour une petite table jointe à une grosse table
    def broadcastIfEnabled(small: DataFrame): DataFrame = {
        if (ConfigLoader.getBoolean("app.optimization.enable-broadcast", true)) broadcast(small)
        else small
    }
}

