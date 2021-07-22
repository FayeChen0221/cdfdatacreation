/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import cdf.shaded.org.apache.avro.Schema;
import cdf.shaded.org.apache.avro.data.TimeConversions.TimestampConversion;
import cdf.shaded.org.apache.avro.io.BinaryDecoder;
import cdf.shaded.org.apache.avro.io.BinaryEncoder;
import cdf.shaded.org.apache.avro.io.DatumWriter;
import cdf.shaded.org.apache.avro.io.Decoder;
import cdf.shaded.org.apache.avro.io.DecoderFactory;
import cdf.shaded.org.apache.avro.io.Encoder;
import cdf.shaded.org.apache.avro.io.EncoderFactory;
import cdf.shaded.org.apache.avro.io.JsonEncoder;
import cdf.shaded.org.apache.avro.specific.SpecificData;
import cdf.shaded.org.apache.avro.specific.SpecificDatumReader;
import cdf.shaded.org.apache.avro.specific.SpecificDatumWriter;
import cdf.shaded.org.apache.avro.specific.SpecificRecordBase;

/**
 *
 * @author from EDPAutomation/matterhorn
 */
public class AvroSerializer<T extends SpecificRecordBase> extends Serializer {

    private static final String SERIALIZE_ERROR = "Can't serialize the message";
    private static final String DESERIALIZE_ERROR = "Can't deserialize the message";
    private DatumWriter<T> datumWriter;
    private SpecificDatumReader<T> reader;
    private Schema schema;

    public AvroSerializer(Class<T> tClass) {
        SpecificData.get().addLogicalTypeConversion(new TimestampConversion());
        this.schema = SpecificData.get().getSchema(tClass);
        this.datumWriter = new SpecificDatumWriter(this.schema);
        this.reader = new SpecificDatumReader(this.schema, this.schema);
    }

    public T deserialize(byte[] data)  {
        T message = null;

        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, (BinaryDecoder) null);
            message = (T) this.reader.read((T) null, decoder);
            return message;
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public T deserialize(String data)  {
        T message = null;

        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(this.schema, data);
            message = (T) this.reader.read((T) null, decoder);
            return message;
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    public byte[] serialize(T data)  {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, (BinaryEncoder) null);
            this.datumWriter.write(data, encoder);
            encoder.flush();
            out.close();
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }

        return out.toByteArray();
    }

    public String serializeToJson(T data)  {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(this.schema, out, true);
            this.serialize(data, out, encoder);
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }

        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private void serialize(T data, ByteArrayOutputStream out, Encoder encoder) {
        try {
            this.datumWriter.write(data, encoder);
            encoder.flush();
            out.close();
        } catch (Exception var5) {
            throw new RuntimeException(var5);
        }
    }

    public T deepCopy(T object) {
        return (T) SpecificData.get().deepCopy(this.schema, object);
    }
}
