/*
 * Spark Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.benchmarks;

import org.apache.commons.cli.*;

/**
 * Created by atr on 11.10.17.
 */
public class GraphXOptions extends TestOptions {
    private Options options;
    private int pageRankIterations;
    private String testName;
    private String graphFile;
    private String warmupGraphFile;

    GraphXOptions(){
        options = new Options();
        pageRankIterations = 4;
        testName = "PageRank";
        graphFile = null;
        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "iterations", true, "number of iteration for the PageRank algorithm, default " + this.pageRankIterations);
        options.addOption("g", "graph", true, "the graph file, default: Null ");
        options.addOption("w", "warmupGraph", true, "the graph file, default: Null ");
        options.addOption("t", "testname", true, "test name, options are : PageRank, default: " + this.testName);
    }

    @Override
    public void parse(String[] args) {
        if (args != null) {
            CommandLineParser parser = new GnuParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                if (cmd.hasOption("h")) {
                    show_help("GraphX", this.options);
                    System.exit(0);
                }
                if (cmd.hasOption("i")) {
                    this.pageRankIterations = Integer.parseInt(cmd.getOptionValue("gi").trim());
                }
                if (cmd.hasOption("g")) {
                    this.graphFile = cmd.getOptionValue("g").trim();
                }
            } catch (ParseException e) {
                show_help("GraphX", this.options);
                errorAbort("Failed to parse command line properties" + e);
            }

            if (this.graphFile == null) {
                errorAbort("PageRank needs a graph file, passed using the -g flag");
            }
        }
    }

    public int getPageRankIterations(){
        return this.pageRankIterations;
    }

    public String getGraphFile(){
        return this.graphFile;
    }

    /* whatever it takes */
    public void setWarmupConfig(){
        /* here it means to swap the input files */
        String temp = this.graphFile;
        this.graphFile = this.warmupGraphFile;
        this.warmupGraphFile = temp;
    }

    /* whatever it takes */
    public void restoreInputConfig(){
        /* we swap again the file names */
        String temp = this.graphFile;
        this.graphFile = this.warmupGraphFile;
        this.warmupGraphFile = temp;
    }

    @Override
    public boolean withWarmup() {
        return false;
    }

    public String getTestName(){
        return this.testName;
    }

    public boolean isTestPageRank(){
        return this.testName.compareToIgnoreCase("pagerank") == 0;
    }
}
