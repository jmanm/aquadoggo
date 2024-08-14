use std::collections::HashMap;

use p2panda_rs::document::DocumentId;
use p2panda_rs::{schema::FieldType, test_utils::fixtures::random_key_pair};
use rstest::rstest;

use crate::aquadoggo_rpc::{CollectionRequest, Document, Field, MetaFilter};
use crate::aquadoggo_rpc::{field::Value, DocumentRequest};
use crate::test_utils::{add_document, add_schema, grpc_test_client, test_runner, TestNode};

// fn match_or_fail<V, F>(value: Option<V>, test_fn: F, msg: &'static str) where F : FnOnce(V) -> () {
//     if let Some(v) = value {
//         test_fn(v);
//     } else {
//         panic!("{}", msg);
//     }
// }

trait Testable {
    fn get_field_map(&self) -> HashMap<String, &Field>;
}

impl Testable for Document {
    fn get_field_map(&self) -> HashMap<String, &Field> {
        let mut result = HashMap::new();
        for field in &self.fields {
            result.insert(field.name.clone(), field);
        }
        result
    }
}

#[test]
fn scalar_fields() {
    test_runner(|mut node: TestNode| async move {
        let key_pair = random_key_pair();

        // Add schema to node
        let schema = add_schema(
            &mut node,
            "schema_name",
            vec![
                ("bool", FieldType::Boolean),
                ("float", FieldType::Float),
                ("int", FieldType::Integer),
                ("text", FieldType::String),
                ("bytes", FieldType::Bytes),
            ],
            &key_pair,
        )
        .await;

        // Publish document on node
        let doc_fields = vec![
            ("bool", true.into()),
            ("float", (1.0).into()),
            ("int", 1.into()),
            ("text", "yes".into()),
            ("bytes", vec![0, 1, 2, 3][..].into()),
        ];
        let view_id = add_document(&mut node, schema.id(), doc_fields.clone(), &key_pair).await;

        let mut test_client = grpc_test_client(&node).await;
        let mut request = DocumentRequest::default();
        request.document_view_id = Some(view_id.to_string());

        let response = test_client.client.get_document(request)
            .await
            .unwrap()
            .into_inner();

        assert!(response.document.is_some());
        let doc = response.document.unwrap();
        assert!(!doc.fields.is_empty());

        let field_map = doc.get_field_map();
        assert_eq!(field_map.len(), doc_fields.len());
        assert!(matches!(field_map.get("bool").unwrap().value, Some(Value::BoolVal(true))));
        if let Some(Value::FloatVal(f)) = field_map.get("float").unwrap().value {
            assert_eq!(1.0, f);
        } else {
            panic!("Didn't get a float!");
        }
        assert!(matches!(field_map.get("int").unwrap().value, Some(Value::IntVal(1))));
        if let Some(Value::StringVal(s)) = &field_map.get("text").unwrap().value {
            assert_eq!(*s, "yes".to_string());
        } else {
            panic!("Didn't get a string!")
        }
        if let Some(Value::ByteVal(b)) = &field_map.get("bytes").unwrap().value {
            assert_eq!(*b, vec![0, 1, 2, 3]);
        } else {
            panic!("Didn't get any bytes!");
        }
    });
}

// Test querying application documents across a parent-child relation using different kinds of
// relation fields.
#[rstest]
fn relation_fields() {
    test_runner(|mut node: TestNode| async move {
        let key_pair = random_key_pair();

        // Add schemas to node
        let child_schema = add_schema(
            &mut node,
            "child",
            vec![("it_works", FieldType::Boolean)],
            &key_pair,
        )
        .await;

        let parent_schema = add_schema(
            &mut node,
            "parent",
            vec![
                (
                    "by_relation",
                    FieldType::Relation(child_schema.id().clone()),
                ),
                (
                    "by_pinned_relation",
                    FieldType::PinnedRelation(child_schema.id().clone()),
                ),
                (
                    "by_relation_list",
                    FieldType::RelationList(child_schema.id().clone()),
                ),
                (
                    "by_pinned_relation_list",
                    FieldType::PinnedRelationList(child_schema.id().clone()),
                ),
            ],
            &key_pair,
        )
        .await;

        // Publish child document on node
        let child_view_id = add_document(
            &mut node,
            child_schema.id(),
            vec![("it_works", true.into())].try_into().unwrap(),
            &key_pair,
        )
        .await;

        // There is only one operation so view id = doc id
        let child_doc_id: DocumentId = child_view_id.to_string().parse().unwrap();

        // Publish parent document on node
        let parent_fields = vec![
            ("by_relation", child_doc_id.clone().into()),
            ("by_pinned_relation", child_view_id.clone().into()),
            ("by_relation_list", vec![child_doc_id].into()),
            ("by_pinned_relation_list", vec![child_view_id].into()),
        ];

        let parent_view_id =
            add_document(&mut node, parent_schema.id(), parent_fields, &key_pair).await;

        // Configure and send test query
        let mut test_client = grpc_test_client(&node).await;
        let mut request = CollectionRequest::default();
        request.schema_id = parent_schema.id().to_string();
        request.meta = Some(MetaFilter {
            owner: None,
            deleted: None,
            edited: None,
            document_id: None,
            view_id: Some(parent_view_id.to_string())
        });
        request.selections.insert("by_relation".into(), true);
        request.selections.insert("by_pinned_relation".into(), true);
        request.selections.insert("by_relation_list".into(), true);

        let response = test_client.client.get_collection(request).await.unwrap().into_inner();
        assert!(!response.documents.is_empty());

        let doc = response.documents.first().unwrap();
        assert!(doc.meta.is_some());
        assert!(doc.cursor.is_some());
        assert_eq!(doc.fields.len(), 3);

        let field_map = doc.get_field_map();
        let relation_field = field_map.get("by_relation".into());
        assert!(relation_field.is_some());

        if let Some(Value::RelVal(relation)) = &relation_field.unwrap().value {
            let relation_field_map = relation.get_field_map();
            assert!(matches!(relation_field_map.get("it_works".into()).unwrap().value, Some(Value::BoolVal(true))));
        } else {
            panic!("No relation value!");
        }

        let pinned_rel_field = field_map.get("by_pinned_relation".into());
        assert!(pinned_rel_field.is_some());

        if let Some(Value::PinnedRelVal(pinned_rel)) = &pinned_rel_field.unwrap().value {
            let pinned_rel_field_map = pinned_rel.get_field_map();
            assert!(matches!(pinned_rel_field_map.get("it_works".into()).unwrap().value, Some(Value::BoolVal(true))));
        } else {
            panic!("No pinned relation value!");
        }

        let relation_list_field = field_map.get("by_relation_list".into());
        assert!(relation_list_field.is_some());

        // Not implemented
        // if let Some(Value::RelListVal(rel_list)) = &relation_list_field.unwrap().value {

        // } else {
        //     panic!("No relation list value!");
        // }
    });
}
