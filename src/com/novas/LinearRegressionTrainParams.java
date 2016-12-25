package com.novas;

/**
 * Created by novas on 16-12-18.
 */
public class LinearRegressionTrainParams {

    public String trainInputPath="/user/mengfanshan/LinearRegression/train/LogisticRegression.data";
    public String modelOutputPath="/user/mengfanshan/LinearRegression/train/output";
    public String predictOutputPath="/user/mengfanshan/LinearRegression/train/predictoutput";
    public String predictInputPath="/user/mengfanshan/LinearRegression/train/LogisticRegression.data";

    public int loopcount=1;
    public double alpha=0.05;
    public int l=50;
    public String regular="L1";
    public int columncount=20;
    public int label=10;

    public String _loopcount="迭代次数";
    public String _alpha="alpha值";
    public String _l="正则系数";
    public String _regular="正则化";
    public String _label="标签列";

}
