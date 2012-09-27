require "spec_helper"

describe Mongoid::Contextual::Aggregate do

  let(:group) do
    { "_id" => "$genre",
      "totalLikes" => { "$sum" => "$likes" }
    }
  end

  let!(:depeche_mode) do
    Band.create(name: "Depeche Mode", genre: "rock", likes: 200)
  end

  let!(:tool) do
    Band.create(name: "Tool", genre: "rock", likes: 100)
  end

  let!(:parliament) do
    Band.create(name: "Parliament", genre: "funk", likes: 50)
  end

  let!(:collection) do
    Band.collection
  end

  describe "#command" do

    let(:criteria) do
      Band.all
    end

    let(:aggregate) do
      described_class.new(collection, criteria, group)
    end

    let(:pipeline) do
      {
        "$group" => group
      }
    end

    let(:base_command) do
      {
        aggregate: "bands",
        pipeline: [pipeline]
      }
    end

    it "returns the db command" do
      aggregate.command.should eq(base_command)
    end

    context "with sort" do
      let(:criteria) do
        Band.order_by(name: -1)
      end

      it "returns the db command with a sort option" do
        aggregate.command[:pipeline][0].should eq(pipeline.merge("$sort" => {'name' => -1}))
      end
    end

    context "with limit" do
      let(:criteria) do
        Band.limit(10)
      end

      it "returns the db command with a limit option" do
        aggregate.command[:pipeline][0].should eq(pipeline.merge("$limit" => 10))
      end
    end

    context "with skip" do
      let(:criteria) do
        Band.skip(20)
      end

      it "returns the db command with a skip option" do
        aggregate.command[:pipeline][0].should eq(pipeline.merge("$skip" => 20))
      end
    end

    context "with only" do
      let(:criteria) do
        Band.only(:name)
      end

      it "returns the db command with a only option" do
        aggregate.command[:pipeline][0].should eq(pipeline.merge("$project" => {"name" => 1}))
      end
    end
  end

  describe "#each" do

    let(:criteria) do
      Band.all
    end

    let(:aggregate) do
      described_class.new(collection, criteria, group)
    end

    let(:results) do
      aggregate.all
    end

    it "iterates over the results" do
      results.should include(
        { "_id" => "rock", "totalLikes" => 300 },
        { "_id" => "funk", "totalLikes" => 50 }
      )
    end
  end
end
