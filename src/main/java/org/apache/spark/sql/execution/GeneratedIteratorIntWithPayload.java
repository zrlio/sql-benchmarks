package org.apache.spark.sql.execution;

/**
 * Created by atr on 22.09.17.
 */

 /* 001 */

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/* 005 */ public final class GeneratedIteratorIntWithPayload extends org.apache.spark.sql.execution.BufferedRowIterator {
    /* 006 */   private Object[] references;
    /* 007 */   private scala.collection.Iterator[] inputs;
    /* 008 */   private scala.collection.Iterator scan_input;
    /* 009 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
    /* 010 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_scanTime;
    /* 011 */   private long scan_scanTime1;
    /* 012 */   private org.apache.spark.sql.execution.vectorized.ColumnarBatch scan_batch;
    /* 013 */   private int scan_batchIdx;
    /* 014 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance0;
    /* 015 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance1;
    /* 016 */   private UnsafeRow scan_result;
    /* 017 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder scan_holder;
    /* 018 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter scan_rowWriter;
    /* 019 */
  /* 020 */   public GeneratedIteratorIntWithPayload(Object[] references) {
    /* 021 */     this.references = references;
    /* 022 */   }
    /* 023 */
  /* 024 */   public void init(int index, scala.collection.Iterator[] inputs) {
    /* 025 */     partitionIndex = index;
    /* 026 */     this.inputs = inputs;
    /* 027 */     scan_input = inputs[0];
    /* 028 */     this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
    /* 029 */     this.scan_scanTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
    /* 030 */     scan_scanTime1 = 0;
    /* 031 */     scan_batch = null;
    /* 032 */     scan_batchIdx = 0;
    /* 033 */     scan_colInstance0 = null;
    /* 034 */     scan_colInstance1 = null;
    /* 035 */     scan_result = new UnsafeRow(2);
    /* 036 */     this.scan_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(scan_result, 32);
    /* 037 */     this.scan_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(scan_holder, 2);
    /* 038 */
    /* 039 */   }
    /* 040 */
  /* 041 */   private void scan_nextBatch() throws java.io.IOException {
    /* 042 */     long getBatchStart = System.nanoTime();
    /* 043 */     if (scan_input.hasNext()) {
      /* 044 */       scan_batch = (org.apache.spark.sql.execution.vectorized.ColumnarBatch)scan_input.next();
      /* 045 */       scan_numOutputRows.add(scan_batch.numRows());
      /* 046 */       scan_batchIdx = 0;
      /* 047 */       scan_colInstance0 = scan_batch.column(0);
      /* 048 */       scan_colInstance1 = scan_batch.column(1);
      /* 049 */
      /* 050 */     }
    /* 051 */     scan_scanTime1 += System.nanoTime() - getBatchStart;
    /* 052 */   }
    /* 053 */
  /* 054 */   protected void processNext() throws java.io.IOException {
    /* 055 */     if (scan_batch == null) {
      /* 056 */       scan_nextBatch();
      /* 057 */     }
    /* 058 */     while (scan_batch != null) {
      /* 059 */       int numRows = scan_batch.numRows();
      /* 060 */       while (scan_batchIdx < numRows) {
        /* 061 */         int scan_rowIdx = scan_batchIdx++;
        /* 062 */         boolean scan_isNull = scan_colInstance0.isNullAt(scan_rowIdx);
        /* 063 */         int scan_value = scan_isNull ? -1 : (scan_colInstance0.getInt(scan_rowIdx));
        /* 064 */         boolean scan_isNull1 = scan_colInstance1.isNullAt(scan_rowIdx);
        /* 065 */         byte[] scan_value1 = scan_isNull1 ? null : (scan_colInstance1.getBinary(scan_rowIdx));
        /* 066 */         scan_holder.reset();
        /* 067 */
        /* 068 */         scan_rowWriter.zeroOutNullBytes();
        /* 069 */
        /* 070 */         if (scan_isNull) {
          /* 071 */           scan_rowWriter.setNullAt(0);
          /* 072 */         } else {
          /* 073 */           scan_rowWriter.write(0, scan_value);
          /* 074 */         }
        /* 075 */
        /* 076 */         if (scan_isNull1) {
          /* 077 */           scan_rowWriter.setNullAt(1);
          /* 078 */         } else {
          /* 079 */           scan_rowWriter.write(1, scan_value1);
          /* 080 */         }
        /* 081 */         scan_result.setTotalSize(scan_holder.totalSize());
        /* 082 */         append(scan_result);
        /* 083 */         if (shouldStop()) return;
        /* 084 */       }
      /* 085 */       scan_batch = null;
      /* 086 */       scan_nextBatch();
      /* 087 */     }
    /* 088 */     scan_scanTime.add(scan_scanTime1 / (1000 * 1000));
    /* 089 */     scan_scanTime1 = 0;
    /* 090 */   }
  /* 091 */ }

