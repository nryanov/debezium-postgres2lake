package io.debezium.postgres2lake.test.parquet;

import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class InMemoryInputFile implements InputFile {
    private final byte[] data;

    public InMemoryInputFile(byte[] data) {
        this.data = data;
    }

    @Override
    public long getLength() throws IOException {
        return data.length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        var bin = new SeekableByteArrayInput(data);

        return new SeekableInputStream() {
            @Override
            public long getPos() throws IOException {
                return bin.available();
            }

            @Override
            public void seek(long l) throws IOException {
                bin.seek(l);
            }

            @Override
            public void readFully(byte[] bytes) throws IOException {
                bin.read(bytes);
            }

            @Override
            public void readFully(byte[] bytes, int i, int i1) throws IOException {
                bin.read(bytes, i, i1);
            }

            @Override
            public int read(ByteBuffer byteBuffer) throws IOException {
                return bin.read(byteBuffer.array());
            }

            @Override
            public void readFully(ByteBuffer byteBuffer) throws IOException {
                bin.read(byteBuffer.array());
            }

            @Override
            public int read() throws IOException {
                return bin.read();
            }
        };
    }
}
