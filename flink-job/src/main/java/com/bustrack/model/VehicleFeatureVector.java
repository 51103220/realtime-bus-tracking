package com.bustrack.model;

import java.io.Serializable;

// 12 feature mỗi xe mỗi giờ — ghi vào MinIO cho PCA offline
public class VehicleFeatureVector implements Serializable {
    public String vehicle;
    public String routeNo;
    public long   windowStartMs;
    public int    hourOfDay;        // 0-23

    // feature tốc độ
    public double eventCount;
    public double avgSpeed;         // trung bình speed không null
    public double maxSpeed;
    public double stdSpeed;         // độ lệch chuẩn
    public double idleFraction;     // tỉ lệ speed < 2 km/h

    // feature không gian
    public double totalDistanceKm;  // tổng haversine

    // feature độ đều sampling
    public double avgSamplingIntervalS;   // giây trung bình giữa các ping
    public double samplingIrregularity;   // std dev của khoảng cách sampling

    // mã hóa giờ dạng cyclic (tránh bất liên tục ở nửa đêm)
    public double hourSin;          // sin(2π * hourOfDay / 24)
    public double hourCos;          // cos(2π * hourOfDay / 24)

    // feature vận hành
    public double headingChanges;   // số lần |delta heading| > 45 độ
    public double ignitionOnFraction;
    public double airconOnFraction;

    public VehicleFeatureVector() {}

    // 12 feature cốt lõi dưới dạng mảng — dùng cho sklearn PCA
    public double[] toArray() {
        return new double[]{
            eventCount, avgSpeed, maxSpeed, stdSpeed, idleFraction,
            totalDistanceKm, avgSamplingIntervalS, samplingIrregularity,
            hourSin, hourCos, headingChanges, ignitionOnFraction
        };
    }
}
