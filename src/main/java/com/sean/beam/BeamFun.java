package com.sean.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class BeamFun {

    private final static List<String> LINES = Arrays.asList(
            "To be, or not to be: that is the question: ",
            "Whether 'tis nobler in the mind to suffer ",
            "The slings and arrows of outrageous fortune, ",
            "Or to take arms against a sea of troubles, ");


    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> words = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        PCollection<Integer> wordLengths = words.apply(
                "ComputeWordLengths",
                ParDo.of(new DoFn<String, Integer>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().length());
                    }
                }));

        PCollection<Integer> wordLengthsLm = words.apply(
                MapElements.into(TypeDescriptors.integers())
                .via((String word) -> word.length()));

        p.run();


    }

}
